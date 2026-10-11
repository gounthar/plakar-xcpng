# Design: XCP-ng integration for Plakar

Status: prototype. This describes what the code in this repository does, and the choices
behind it. Where a choice still rests on an assumption rather than a test, it says so.

## What a Plakar integration is

A Plakar integration is a pair of executables, an importer (backup) and an exporter
(restore), plus a `manifest.yaml`. Plakar starts them as child processes and talks to them
over gRPC on stdio, through `go-kloset-sdk`.

- The importer sends `Record{Pathname, FileInfo, Reader}` values on a channel. Plakar
  chunks each reader's bytes with FastCDC (512 KiB min, 1 MiB average, 8 MiB max) and
  stores only the chunks it has not seen before. Deduplication is done by Plakar, not by
  the connector. All the connector has to do is produce bytes that stay the same when the
  underlying data has not changed.
- The exporter receives the same records back on a channel, and sends one result per
  record.

The layout here follows `PlakarKorp/integration-proxmox`: `importer/`, `exporter/`,
`internal/`, `plugin/{importer,exporter}/main.go`, one JSON schema per connector, a
`Makefile` that builds the two binaries and packages them with `plakar pkg create`.

## Why per-VDI raw export

Measured with plakar 1.1.7 on an 8 GiB VDI holding 3.4 GiB of files, one run per cell:

| Export | 2nd backup, unchanged | 2nd backup, 184 MiB changed |
|---|---|---|
| `/export_raw_vdi?format=raw` | +2.3 MB | +64.5 MB |
| `/export` (XVA, uncompressed) | +1.6 GB | +1.6 GB |

The XVA format numbers its objects `Ref:N` with a fresh `N` on each export, and that name
appears in a tar header every 1 MiB, the same size as an average chunk, so every chunk
differs. Raw export has no such per-export identifier. The raw path needs no change to
XCP-ng.

## Talking to XCP-ng

The connector talks to XAPI directly: the JSON-RPC endpoint (`/jsonrpc`) for API calls,
and the HTTP handlers `/export_raw_vdi` and `/import_raw_vdi` for disk data. It does not
go through Xen Orchestra. That keeps the dependency to one host and matches the setup
used for the measurements. Going through Xen Orchestra's REST API instead is a possible
later variant, not a requirement.

## Configuration

Shared by importer and exporter:

| Key | Meaning |
|---|---|
| `location` | `xcpng://host[:port]` of the pool master |
| `username` | XAPI user, usually `root` |
| `password` | XAPI password. Plakar stores connector configuration in its own config store; never put it in this repository |
| `tls_insecure` | `true` to skip TLS certificate verification. XCP-ng ships a self-signed certificate by default |

Importer selection, exactly one of:

| Key | Meaning |
|---|---|
| `vm_uuid` | one VM by UUID |
| `vm_name` | every VM whose name-label is exactly this |
| `vm_tag` | every VM carrying this tag |
| `stale_snapshot_age` | destroy leftover snapshots older than this, default `24h`, `0` disables |

Exporter:

| Key | Meaning |
|---|---|
| `sr_uuid` | SR to create the restored VDIs on. Defaults to the pool's default SR |
| `name_suffix` | appended to the restored VM's name-label, default ` (restored)` |
| `start_on_restore` | start the VM once restored, default `false` |
| `keep_mac` | give the VIFs their original MAC addresses, default `false` |
| `keep_tags` | give the VM the original's tags, default `false` |

## Backup (importer)

For each selected VM:

1. Take a VM snapshot (`VM.snapshot`). A raw export of a VDI attached to a running VM is
   not crash-consistent; a snapshot is. The snapshot is taken for halted VMs too, so that
   the VM can be started during the backup without changing what is read.
2. For each VBD of the snapshot of type `Disk`, emit one record:
   `/<vm-uuid>/<userdevice>-<vdi-uuid>.raw`, streamed from
   `/export_raw_vdi?vdi=<snapshot VDI>&format=raw`. The size is the VDI's `virtual_size`.
   The UUIDs in the path are the **original** VM's and the **original** VDI's (the
   snapshot VDI's `snapshot_of`), so the path stays the same from one backup to the next.
   CD drives are skipped.
3. Emit `/<vm-uuid>/vm.json`: the VM record and, for each VBD and VIF, the record with the
   referenced VDI or network resolved to UUID and name. This is what the restore uses to
   rebuild the VM. It is small and is stored whole each time.
4. Wait for Plakar to acknowledge every record of this VM, then destroy the snapshot and
   its VDIs.

Step 4 only runs if the connector is still alive. If plakar is killed or the machine
running it loses power, the snapshot stays on the host and keeps its storage. So every
snapshot the connector takes is marked with `plakar_xcpng_snapshot=<its own UUID>` in its
`other_config` (XAPI copies `other_config` on clone and revert, and a copied marker no
longer matches the UUID of the object it lands on),
and before step 1 the importer destroys the VM's marked snapshots older than
`stale_snapshot_age`, with their VDIs, destroying only VDIs that are snapshot copies. Snapshots without the marker are never touched,
whatever their name. The age threshold exists because two backups of the same VM can
overlap; it has to be longer than the longest backup of one VM.

Step 4 relies on `FLAG_NEEDACK`: with it, Plakar sends one result per record once that
record has been fully read and chunked, and the gRPC layer forwards those results to the
plugin (`go-kloset-sdk` `importer.go`, `integration-grpc` `importer/importer.go`).
Without it, `Import()` could return and the snapshot be destroyed while a VDI stream is
still being read. Destroying a VM snapshot leaves its VDIs behind, so they are destroyed
explicitly.

The data stream is opened lazily, when Plakar starts reading the record, so only as many
exports run at once as Plakar has workers.

## Restore (exporter)

Nothing guarantees that `vm.json` arrives before the disks, so:

1. Each `.raw` record is restored as soon as it arrives: create a VDI of the record's size
   on the target SR, then stream the record into `/import_raw_vdi?vdi=...&format=raw`.
   The original VDI UUID from the pathname is kept in a map to the new VDI.
2. Each `vm.json` is kept in memory.
3. Once all records are in, for each `vm.json`: create the VM from the saved record
   (name, memory, vCPUs, boot and platform settings), attach the restored VDIs with their
   saved `userdevice`, `bootable` and `mode`, and recreate the VIFs on networks with the
   same UUID, falling back to the same name. A network that cannot be found is skipped
   with an error on that record.
4. Send the results: per `.raw` as it completes, per `vm.json` at the end.

The restored VM is always a new VM. The prototype never overwrites an existing one.

MAC addresses: by default the restored VIFs get new MACs and the VM's `mac_seed` is
dropped, because the common case of a test restore puts the copy on the same network as
the original, and two VMs with one MAC clash. For disaster recovery, where the original is
gone and DHCP reservations, firewall rules or guest network configuration are tied to the
old MACs, set `keep_mac=true`: VIFs whose MAC was set explicitly get it back, and
`mac_seed` is kept so autogenerated MACs come out the same.

Tags: by default the restored VM has none. With the original's tags, a backup that
selects VMs by `vm_tag` would back up the restored copy as well; the first lab restore
showed exactly that. `keep_tags=true` restores them, for disaster recovery where the copy
replaces the original and should stay under the same backup selection.

## Not in the prototype

- Incremental export (changed block tracking). Every backup reads every VDI in full;
  Plakar only stores what changed, but the read is still the whole disk. Same as the
  Proxmox connector.
- Memory state (`checkpoint`). Snapshots are disk-only.
- Restoring onto an existing VM, or into a different pool with different network names
  beyond the name fallback above.
- VM selection by pool, or all VMs.

## Packaging

The manifest declares the class `hypervisor` with no subclass. The `plakar pkg` module
only accepts `proxmox` and `vmware` as hypervisor subclasses (PlakarKorp/pkg v1.1.7,
`var.go`), so an `xcpng` subclass needs a change there.

## Measured

plakar 1.1.7, one run per row, one test VM with an 8 GiB disk holding 3.4 GiB of files:

| | stored | wall |
|---|---|---|
| first backup | 1.58 GB | 2m04 |
| second backup, unchanged | 0.74 MB | 1m41 |
| third backup, 180 MiB of blocks changed | 62.9 MB | 1m37 |
| restore of the first backup | disk byte-identical (sha256) | 2m06 |

The importer's snapshot and its VDI were gone after the backup.

A second run used a running Debian 12 VM with three disks (boot, the 8 GiB test disk, an
empty 1 GiB disk), selected by `vm_name`, with the source pointed at a pool member:

| | stored | wall |
|---|---|---|
| first backup | 2.09 GB | 2m16 |
| second backup, guest running and idle | 1.56 MB | 2m30 |
| restore with `keep_mac` and `keep_tags` | data disks byte-identical, boots | 3m02 |

The restore with `keep_mac` and `keep_tags` brought back the same MAC and tags; one with
default options gave no tags and a new MAC. A backup killed while streaming left its
marked snapshot behind, and the next backup (with `stale_snapshot_age=1s`) destroyed it
and its VDIs, while an unmarked snapshot with the same kind of name stayed.

Not exercised: a guest writing heavily during the backup (the test guest was idle),
`start_on_restore`, UEFI and vTPM guests.

## Open questions


- Whether restoring the VM from the saved record covers every guest type, in particular
  UEFI guests (NVRAM is in the VM record) and vTPM.
- Whether `format=vhd` with its sparse map would read less of a thin-provisioned VDI
  without hurting deduplication. Raw is what was measured.
