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

With `vm_name` or `vm_tag`, the VMs are backed up one after another into one snapshot, and
an error on any of them fails the whole run: plakar commits no snapshot and exits 1, so
the VMs already read are not kept either. This is deliberate. A failed VM could instead
be recorded as an error entry in a snapshot that keeps the others, but plakar 1.1.7 then
exits 0, and a scheduled backup that lost a VM would look successful: `restore` and
`check` of that snapshot also succeed without mentioning it, and only `plakar info`
counts the error. To keep one
failing VM from holding back the rest, give each VM its own source (`vm_uuid`).

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
| `allow_incomplete_disks` | restore a VM whose backup holds a disk shorter than the disk was, default `false` (see below) |

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

If xapi stops answering during step 4, as during a toolstack restart, the importer waits
for it for up to three minutes, logging in again when the restart has dropped the session.
The VDIs are destroyed before the snapshot, so a VDI that cannot be destroyed leaves the
marked snapshot (see below) for the sweep to find. If the snapshot still cannot be
destroyed and every record of the VM was stored, the backup is kept and the importer
prints a warning naming the snapshot: the sweep removes it at the first backup of the same
VM that starts once it is older than `stale_snapshot_age`, or it has to be destroyed by
hand, which is always the case with `stale_snapshot_age=0`.
A backup that failed for another reason still fails, with the cleanup error added.

Step 4 only runs if the connector is still alive, and an interrupted plakar does not
leave it alive: plakar 1.1.7 starts plugins with `exec.CommandContext` and cancels that
context on SIGINT or SIGTERM, which sends the plugin SIGKILL. So when it takes its first
snapshot, the importer starts a copy of itself as a watchdog, which ignores SIGINT,
SIGTERM and SIGHUP. On Unix the watchdog runs in a session of its own, so a signal sent
to plakar's whole process group, such as `kill -9 -<pgid>` from a shell, does not reach
it either. The importer tells it over a pipe which snapshots exist (the
connector configuration goes over the same pipe, not the command line, because it holds
the password). If the pipe closes while a snapshot is still listed, the importer died, and
the watchdog destroys that snapshot and its VDIs, after checking the marker below.

The marker is set once `VM.snapshot` returns, but xapi finishes a snapshot even when the
caller has stopped waiting: plakar interrupted, the connection lost, or the call's own
one-hour bound reached. So each snapshot gets a label with a random part, and the importer
lists the VM and that label with the watchdog before calling `VM.snapshot`, swapping it for
the snapshot's UUID once marked. When the call ends without an answer from xapi, the
importer, or the watchdog if the importer died, waits until the VM no longer shows a
`snapshot` operation, then destroys the VM's snapshots carrying exactly that label. xapi
names the snapshot record before it has copied its VBDs, VIFs and vTPMs, and ends the
operation only once the snapshot is whole, so looking earlier could miss a half-made
snapshot or destroy it while xapi works on it. An error returned by xapi means it took no
snapshot: it deletes the record and the cloned disks before answering, and nothing is
looked for. (Read in xen-api, `xapi_vm_clone.ml` `clone` and `xapi_vm_helpers.ml`
`with_vm_operation`, upstream master. On an XCP-ng 8.3 host, plakar interrupted while
`VM.snapshot` ran on a halted VM with eight disks: before this change the snapshot and its
disks stayed, unmarked; with it the watchdog destroyed them, twice out of two. The
importer's own lookup, after a timeout or a lost connection, was not run on a host.) The importer waits up
to ten minutes; past that the label stays listed and the watchdog tries again when the
importer exits, within its own ten-minute bound.
VDI.destroy is retried for up to two minutes on `VDI_IN_USE`: xapi keeps a VDI attached
for a moment after an export is cut off.

Under systemd the watchdog is in the unit's cgroup, and its own session does not take it
out. With the default `KillMode=control-group`, `systemctl stop` sends SIGTERM to every
process of the unit, which the watchdog ignores, then waits for the cgroup to empty, and
sends SIGKILL to what is left after `TimeoutStopSec`. So the timeout has to cover the
watchdog's cleanup, which is bounded at ten minutes: up to three for xapi to come back,
and up to two per disk on `VDI_IN_USE`. That bound starts when plakar drops the
connector, a little after systemd's stop timer, so `TimeoutStopSec=11min` covers it with
room to spare. It costs nothing in the usual case, as the stop returns once the cgroup is
empty. `KillMode=mixed`
sends SIGKILL to the rest of the cgroup as soon as plakar exits, so it always kills the
watchdog. `systemctl kill -s KILL` and an OOM kill of the whole unit kill it too, and so
does stopping a container, whose PID namespace dies with its first process.

The watchdog does not help in those cases, or if the machine running plakar loses power
or the watchdog itself is killed; the snapshot then stays on the host and keeps its
storage. So every
snapshot the connector takes is marked with `plakar_xcpng_snapshot=<its own UUID>` in its
`other_config` (XAPI copies `other_config` on clone and revert, and a copied marker no
longer matches the UUID of the object it lands on),
and before step 1 the importer destroys the VM's marked snapshots older than
`stale_snapshot_age`, with their VDIs, destroying only VDIs that are snapshot copies. It
takes a snapshot with no marker key at all too, when its name is exactly a label as chosen
below, `plakar-<YYYYMMDDTHHMMSSZ>-<8 lowercase hex>`: that is what is left when the plugin
and its watchdog are both killed while `VM.snapshot` runs, so the marker was never set.
Any other unmarked snapshot is left alone, and so is one whose marker holds another UUID,
whatever its name. xapi names a snapshot before it has finished taking it, so while the VM
shows a snapshot operation, which may be another backup's still in `VM.snapshot`, the sweep
leaves unmarked snapshots for the next backup. The risk taken is a snapshot someone else
named exactly like that, on a VM this connector backs up, or one of ours that a user kept
and removed the marker from without renaming it: renaming it keeps it. The age threshold exists because two backups of the same VM can
overlap; it has to be longer than the longest backup of one VM. The age is measured on the
pool master's clock (`host.get_servertime`), which stamped `snapshot_time`, so a plakar host
whose clock is off cannot sweep a fresh snapshot or keep an old one. A marked snapshot with a
disk attached anywhere is left in place with a warning, whatever its age: an export plugs
the VDI into dom0 for as long as it streams, so this is a backup still reading it. That
check does not see a backup between two disks, or before its first export starts, when
nothing is attached, so the age threshold still matters. A sweep that fails (xapi refuses
a destroy, or cannot list the snapshots) prints a warning and the backup goes on: the
snapshots it could not remove are tried again at the next backup of the VM, and with
`vm_tag` the other VMs are still backed up.

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
   Before any of this, each restored disk's size is checked against the `virtual_size`
   in `vm.json`. Older versions could store a disk whose export was cut short; such a VM
   is refused and its restored VDIs destroyed. With `allow_incomplete_disks=true`, a
   short disk is grown to its recorded size with `VDI.resize` and the VM is restored with
   a warning naming the disk: the missing end reads as zeroes, so the guest may find a
   damaged filesystem. That holds only where a new VDI reads as zeroes, so on the SRs
   listed in step 1 the disk is still refused. A disk larger than recorded is always
   refused. The end of such a stored disk is not all disk data: the export stopped
   wherever it stopped, often mid-sector, and the file can end with the HTTP error page
   xapi wrote after its 200. With the option set, the restore keeps the last 64 KiB of
   each disk back until the stream ends, removes an error page that ends exactly at the
   end of the file, and stops at the last whole sector before it. The warning says how
   many bytes that dropped. The page is recognised by its shape only, and a complete
   disk can end with guest data of the same shape. Which one it was is known only once
   `vm.json` is read, after the disk is written, so the removed bytes are kept: when
   the stored size equals the recorded `virtual_size`, they are written back, as
   stored, into the VDI in one chunk of the chunked import.
4. Send the results at the end, once it is known which disks have a VM. A `.raw` that
   no `vm.json` lists (a restore of the disk file alone, a file beside a `vm.json` that
   does not name it, or one beside a `vm.json` that could not be read) has its VDI
   destroyed and its result is an error, since the restore makes VMs, not loose disks.
   The disks of a VM that was not restored report that too. A failed `.raw` is reported
   as it fails.

A restore that fails partway removes what it made. A VM that cannot be made, attached
or checked for Secure Boot is destroyed with its restored VDIs (`VM.destroy` removes the
VBDs, not the VDIs), and so are the disks of the VMs an interrupt stops before. A
`VDI.create` or `VM.create` that gets no answer may still have been carried out by
xapi, so each create carries a label unique to the restore: the VDI's name until the
create answers, a `plakar_restore` key in the VM's `other_config` until the VM is made.
After an error that is not xapi's own, the exporter waits until the SR shows no
`vdi_create` (`VDI.create` holds it until the VDI's record exists), or looks for the VM
again for up to 2 minutes (xapi shows no operation while it creates one), and destroys
what carries the label. On a busy SR, where other clients keep `vdi_create` running,
the VDI is looked for anyway once the wait runs out. Every cleanup call is retried
for up to 3 minutes while xapi does not answer, with a new session after a toolstack
restart, and each attempt has its own one-minute bound. Measured on XCP-ng 8.3.0 (xapi-core 26.1.19): a `VDI.create` cut at 50 to 200 ms was
finished by xapi each time and found; a `VM.create` cut at 1 ms was found within 20 ms.
Not covered: xapi stopping between the SM's create and the VDI's record (an `SR.scan`
brings the volume back), and a cleanup call that itself fails, which the result names:
the VM's, and for a VDI that could not be destroyed, its disk's own.
A VM that is made but fails `VM.start` is kept: it is complete, only not running.

An interrupted plakar kills the exporter with SIGKILL, as it does the importer, so the
exporter starts the same kind of watchdog at its first create (`--xcpng-restore-watchdog`).
Each VDI and VM is listed with it before its create is sent, by SR and label or by name
and label, then by reference once the create answers, and unlisted once it is destroyed
or kept: a VM is kept, with its disks, once its Secure Boot check passes or holds it
back, before `VM.start`. Each write into a VDI runs under a xapi task named
`plakar import_raw_vdi <label>`, with a label of its own, listed for as long as the write runs, so that a task left by an interrupted
write is destroyed by name. `task.destroy` only marks a task still pending, and a task
the exporter created and was killed before using never ends, so the watchdog gives a
pending task 30 s to end (a cut import's does within about 2 s), then sets it cancelled
and destroys it. If the pipe closes while something is listed, the watchdog
destroys the VMs first, then the VDIs and tasks, with the same label searches and retries
as above. A task a killed watchdog leaves is removed by xapi itself: about an hour after it
ended, or a day after it was created if the write never started (`completed_task_timeout`
and `pending_task_timeout` in xapi's `xapi_globs.ml`). A cleanup that fails while the exporter is alive leaves its object listed, so the
watchdog tries it once more when the exporter exits. A VDI xapi still holds (`VDI_IN_USE`,
as after an import whose connection was cut) is retried by the exporter itself, all of a VM's held disks together, for up to
3 minutes; what is still held then, or held when the restore was interrupted, is left to the watchdog, and the disk's
error says so. The exporter waits 30 s for the watchdog when it closes, which covers the usual hold
of about a second; a watchdog still at work then goes on in the background, and the exporter says so with
the restore ID, so a disk held for long does not keep `plakar restore` running for the watchdog's
10 minutes (#85: 13 min with a disk held by dom0). Where plakar's exit kills what it leaves running
(`KillMode=mixed`, a container whose first process is plakar), what the watchdog had not destroyed
stays, named by that ID. The importer still waits for its watchdog. The watchdog reports each object it destroys as left by the restore, interrupted or not. What a killed watchdog, or a
machine that loses power, leaves behind is named by the label or by the disk's file
name (`<n>-<VDI UUID>.raw`). Starting the watchdog prints the restore ID,
`plakar-restore-<time>-<random>`, which every label starts with and every restored VDI's
description holds. It has to be said then: Ctrl-C ends the exporter at once, since it is in
plakar's process group, so once the watchdog is killed too, no process is left to say it
(#56: disk 0 left, nothing printed). A watchdog whose 10 minutes run out names each object
it could not destroy and why, then says it stopped trying and where those objects are
found by that ID, which the restore printed too long before to be at hand (#83). It logs
out with a few seconds of its own, its time being spent, and drops that error.

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

Each call in these steps is retried for up to 3 minutes while xapi does not answer, so a
dropped connection does not throw away disks that may have taken hours to copy. Any other
error (a call still unanswered after that, a clearing xapi refuses) fails the VM's restore
and destroys the VM and its disks: a VM whose Secure Boot state was not checked is not a
restored VM.

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
afterwards completed and left neither a snapshot nor a watchdog process. Before the
watchdog had a session of its own, SIGKILL sent to plakar's whole process group
fifteen seconds into streaming killed the watchdog too and left the snapshot and its
VDI; with it, the watchdog destroyed both.

Under systemd, with plakar run as a transient user unit and the unit stopped fifteen
seconds into streaming a 16 GiB VDI, once per setting: with `TimeoutStopSec=5min` the
watchdog destroyed the snapshot 1.1 seconds after plakar exited and `systemctl stop`
returned after 1.3 seconds, leaving nothing. With `TimeoutStopSec=1s` systemd killed the
watchdog half way, after it had destroyed the VDI, and the marked snapshot stayed. With
`KillMode=mixed`, and with `systemctl kill -s KILL`, the watchdog was killed at once and
the snapshot and its VDI stayed. The OOM and container cases were not run.

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

Disk allocation on restore, ext SR on XCP-ng 8.3, a 3 GiB disk with 725 non-zero 2 MiB
blocks out of 1536, one run each:

| restore write | VHD blocks allocated | on disk | sent | import |
|---|---|---|---|---|
| every byte (`format=raw`, before #21) | 1536 of 1536 | 3.0 GiB | 3 GiB | 41.7 s |
| zero blocks left out (`chunked=true`) | 725 of 1536 | 1.42 GiB | 1.42 GiB | 20.2 s |

The second row was run both as a single import and end to end (backup of a VM with that
disk, restore to a new VM); both times the restored disk was sha256-identical to the
source and `vhd-util check` passed. Not tried on hardware: LVM SRs, which keep the full
write, and file SRs other than ext.

## Open questions


- Whether restoring the VM from the saved record covers every guest type. UEFI guests
  (NVRAM is in the VM record) and vTPM state round-trip on XCP-ng 8.3; older targets
  were not tried.
- Whether `format=vhd` with its sparse map would read less of a thin-provisioned VDI
  without hurting deduplication. Raw is what was measured.
