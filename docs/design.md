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

Importer options:

| Key | Meaning |
|---|---|
| `stale_snapshot_age` | destroy leftover snapshots older than this, default `24h`, `0` disables |
| `vtpm` | `include` or `skip` the VM's vTPM state. No default: a VM with a vTPM is refused until it is set (see below) |

Exporter:

| Key | Meaning |
|---|---|
| `sr_uuid` | SR to create the restored VDIs on. Defaults to the pool's default SR |
| `name_suffix` | appended to the restored VM's name-label, default ` (restored)` |
| `start_on_restore` | start the VM once restored, default `false` |
| `keep_mac` | give the VIFs their original MAC addresses, default `false` |
| `keep_tags` | give the VM the original's tags, default `false` |
| `keep_secureboot_update` | let the target pool replace the VM's Secure Boot keys at next boot, default `false` (see below) |

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

Step 4 only runs if the connector is still alive, and an interrupted plakar does not
leave it alive: plakar 1.1.7 starts plugins with `exec.CommandContext` and cancels that
context on SIGINT or SIGTERM, which sends the plugin SIGKILL. So when it takes its first
snapshot, the importer starts a copy of itself as a watchdog, which ignores SIGINT,
SIGTERM and SIGHUP. The importer tells it over a pipe which snapshots exist (the
connector configuration goes over the same pipe, not the command line, because it holds
the password). If the pipe closes while a snapshot is still listed, the importer died, and
the watchdog destroys that snapshot and its VDIs, after checking the marker below.
VDI.destroy is retried for up to two minutes on `VDI_IN_USE`: xapi keeps a VDI attached
for a moment after an export is cut off.

The watchdog does not help if the machine running plakar loses power or the watchdog
itself is killed; the snapshot then stays on the host and keeps its storage. So every
snapshot the connector takes is marked with `plakar_xcpng_snapshot=<its own UUID>` in its
`other_config` (XAPI copies `other_config` on clone and revert, and a copied marker no
longer matches the UUID of the object it lands on),
and before step 1 the importer destroys the VM's marked snapshots older than
`stale_snapshot_age`, with their VDIs, destroying only VDIs that are snapshot copies. Snapshots without the marker are never touched,
whatever their name. The age threshold exists because two backups of the same VM can
overlap; it has to be longer than the longest backup of one VM.

vTPM: the TPM state holds whatever the guest sealed to it, typically the key that unlocks
its encrypted disk. Storing it puts that key in the same backup as the disk, protected only
by the store's encryption; leaving it out means a restored guest cannot unseal anything and
falls back to its recovery key, if it has one. Neither is a safe default, so the importer
checks the VM for a vTPM before taking the snapshot and fails with a message naming the
option until `vtpm` is set. With `include`, `vm.json` gets a `vtpms` list
(`is_unique` and the base64 `contents` from `VTPM.get_contents`), read from the snapshot so
it matches the disks. The exception is a vTPM with `is_unique=true`: XAPI never copies the
state of a unique vTPM, so the snapshot's copy is empty, and its state is read from the VM
itself, a moment after the disks were snapshotted. With `skip`, the list is empty.

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
   Where a new VDI reads as zeroes, the stream uses xapi's chunked form of that call
   (`&chunked=true`: an offset and length before each piece of data) and leaves out
   every 2 MiB block that is all zeroes, so a thin SR allocates only the blocks that
   hold data. The SRs where a new VDI is not zeroed follow xapi's own rule
   (`must_write_zeroes_into_new_vdi`: LVM without `use_vhd`, EqualLogic thick,
   StorageLink); there every byte is written, zeroes included.
   The original VDI UUID from the pathname is kept in a map to the new VDI.
2. Each `vm.json` is kept in memory.
3. Once all records are in, for each `vm.json`: create the VM from the saved record
   (name, memory, vCPUs, boot and platform settings), attach the restored VDIs with their
   saved `userdevice`, `bootable` and `mode`, and recreate the VIFs on networks with the
   same UUID, falling back to the same name. A network that cannot be found is skipped
   with an error on that record.
   A `vtpms` entry is recreated with `VTPM.create` and `VTPM.set_contents`; a failure
   fails that VM's restore rather than leaving a VM that boots without its TPM. A unique
   vTPM is recreated unique with the original's state, which gives the copy the same TPM
   identity and sealed secrets: that is what a disaster-recovery restore needs, and what
   `is_unique` exists to prevent while the original runs, so the restore warns.
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

Secure Boot: the VM's `platform` (with `secureboot`) and `NVRAM` are restored as they
were, never rewritten. Once a VM has booted, its NVRAM holds its own PK, KEK, db and dbx,
and varstored loads them as they are: the target pool's certificates are only used for a
VM's first boot, or when XAPI marks the VM for a certificate update. So after creating the
VM, and before starting it, the exporter:

- clears the certificate update XAPI may have marked at `VM.create` (target pool with
  `auto_update_vm_secureboot_certificates`, NVRAM with only the 2011 Microsoft
  certificates). The update replaces PK, KEK and db, which changes what the guest
  measures, so a key sealed to the restored vTPM may no longer unseal.
  `keep_secureboot_update=true` keeps the update. Leaving it off has a cost: the VM keeps
  the 2011 Microsoft certificates, which expire in 2026, and may later refuse boot
  components signed only with the 2023 ones. So the exporter warns when it clears the
  update, and the option is the one to set for VMs without a sealed key, or once the
  BitLocker recovery key is at hand;
- asks `VM.get_secureboot_readiness`, and:
  - `ready`: nothing to do;
  - `ready_no_dbx`, `setup_mode` (Secure Boot requested but not enforced) or
    `certs_incomplete` (PK, but KEK or db missing): the NVRAM is the backed-up VM's, so
    the copy is in the same state as the original. It restores and starts, with a warning
    on standard error. A VM with PK and db but no KEK still booted in the lab;
  - `first_boot` (the VM never booted, so the pool gives it its keys) on a pool whose
    master has no complete certificate set, or whose hosts hold different certificates
    (compared file by file: each host builds its tar in its own order): the VM is restored
    but not started, and its `vm.json` result is an error saying why. There is no option to start it anyway: installing the pool certificates
    and starting it is the fix, and nothing is lost by waiting.

A restore never turns Secure Boot off: that would silently weaken a control the owner
enabled, and it changes PCR7 too.

## Not in the prototype

- Incremental export (changed block tracking). Every backup reads every VDI in full;
  Plakar only stores what changed, but the read is still the whole disk. Same as the
  Proxmox connector.
- Memory state (`checkpoint`). Snapshots are disk-only.
- Restoring onto an existing VM, or into a different pool with different network names
  beyond the name fallback above.
- VM selection by pool, or all VMs.
- Thin restores. A restored VDI is written with `format=raw`, and on the one thin SR
  tried (ext, XCP-ng 8.3) every block was allocated, zeros included: a disk with 725
  non-zero 2 MiB blocks out of 1536 came back with all 1536 allocated (#21). Other SR
  types were not tried; LVM SRs are thick anyway.

## Packaging

The manifest declares the class `hypervisor` with no subclass. The `plakar pkg` module
only accepts `proxmox` and `vmware` as hypervisor subclasses (PlakarKorp/pkg v1.1.7,
`var.go`), so an `xcpng` subclass needs a change there.

`go-kloset-sdk` and `integration-grpc` carry the plugin's gRPC contract with plakar.
Dependabot groups them into one pull request; before merging one, run a backup and a
restore with the plakar release the measurements below were taken with, since a clean
build does not show the two sides still agree.

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

With the watchdog, a backup interrupted with SIGTERM, and another with SIGINT, five
seconds into streaming an 8 GiB VDI left no snapshot and no snapshot VDI; the watchdog
was done about two seconds after plakar exited. A first version without the
`VDI_IN_USE` retry destroyed the snapshot but left its VDI. An uninterrupted backup
afterwards completed and left neither a snapshot nor a watchdog process.

A running Debian 12 UEFI guest with a vTPM: with `vtpm` unset the backup was refused
before any snapshot was taken; with `include` the restored VM got a vTPM whose contents
were sha256-identical to the stored state; with `skip` the restored VM had none.

The same with Secure Boot on and a guest that seals a random secret at first boot, both in
a TPM NV index and with `systemd-creds --with-key=tpm2 --tpm2-pcrs=7`, then reports at each
boot whether it unseals:

| vTPM | restored guest |
|---|---|
| `is_unique=false`, `include` | both unseal, PCR7 identical to the original's |
| `is_unique=false`, `skip` | no TPM device |
| `is_unique=true`, `include`, state read from the snapshot (before #19) | backed up with an empty state; restored without a warning; neither unseals |
| `is_unique=true`, `include`, state read from the VM | both unseal, PCR7 identical; the restore warns that the copy shares the original's TPM state |

Secure Boot, restored with `start_on_restore=true` on XCP-ng 8.3 (varstored 1.3.4):

| backed-up VM | restore |
|---|---|
| never booted, pool with its certificates | started, took the pool's keys |
| booted | started, no warning |
| setup mode (no PK) | started, warning |
| PK and db, no KEK | started, warning, booted to a login prompt |
| never booted, pool master missing `KEK.auth` | restored halted, error on its `vm.json` |
| KEK with only the 2011 certificate, pool with `auto_update_vm_secureboot_certificates` | update cleared, warning; with `keep_secureboot_update=true` the update stayed and one boot installed the pool's keys |

The last row again with the sealing guest, its KEK reduced to the 2011 certificate before
first boot: with the default restore, the KEK stayed as backed up, PCR7 matched the
original's, and both secrets unsealed. With `keep_secureboot_update=true`, the first boot
installed the pool's KEK, PCR7 changed, the NV index secret still unsealed and the
PCR7-bound one did not ("policy digest does not match"). That is the cost the warning
describes; a BitLocker volume bound to PCR7 may ask for its recovery key in the same way
(Windows was not tested).

The vTPM's dictionary-attack lockout counter is part of the state that is backed up. A
backup of a running VM captures a vTPM that has not been shut down in order, so the
restored copy counts one failed authorization at its first start: its counter read 1
where the original's read 0. A backup of a VM halted with a clean shutdown gave a copy
whose counter read 0. The test guest's vTPM locks at 3 (`TPM2_PT_MAX_AUTH_FAIL`) and
forgets one failure every 1000 s, so a copy restored from a running VM whose counter
already stands at 2 is locked at first boot, and a sealed secret will not unseal until
the lockout expires. In the run above the default-restore copy hit exactly that: one
forced reboot of the original before the backup, the copy's first start, then one forced
reboot of the copy. It unsealed both secrets once the lockout ended. For a VM
whose guest unseals at boot, back it up halted, or expect one try to be spent.

Not exercised: a guest writing heavily during the backup (the test guest was idle), and a
pool whose hosts hold different Secure Boot certificates (the comparison has a unit test
only).

## Open questions


- Whether restoring the VM from the saved record covers every guest type. UEFI guests
  (NVRAM is in the VM record) and vTPM state round-trip on XCP-ng 8.3; older targets
  were not tried.
- Whether `format=vhd` with its sparse map would read less of a thin-provisioned VDI
  without hurting deduplication. Raw is what was measured.
