# plakar-xcpng

Exploring a [Plakar](https://github.com/PlakarKorp/plakar) integration for XCP-ng: back up
XCP-ng VMs into a Plakar repository and restore them, with Plakar's deduplication working
across successive backups.

This is a proof of concept, written in spare time to find out whether such a connector can
work. It is not a Vates product, and Vates does not support it.

## Status

Prototype. The connector backs up and restores a VM end to end through plakar 1.1.7, on
XCP-ng 8.3. It talks to xapi directly; Xen Orchestra is not needed. One run on a single
test VM (8 GiB disk, 3.4 GiB of files), each backup reading the whole disk:

| | stored | wall |
|---|---|---|
| first backup | 1.58 GB | 2m04 |
| second backup, unchanged | 0.74 MB | 1m41 |
| third backup, 180 MiB of blocks changed | 62.9 MB | 1m37 |
| restore of the first backup | new VM, disk byte-identical (sha256) | 2m06 |

See `docs/design.md` for how it works and what it does not do yet.

## Build and install

There is no release yet: build the package from source (Go 1.26).

```sh
make build                 # xcpngImporter, xcpngExporter
make package               # xcpng_<version>_<os>_<arch>.ptar
plakar pkg add -allow-unsigned ./xcpng_v0.1.0_linux_amd64.ptar
plakar source add myvm xcpng://pool-master password=... tls_insecure=true vm_uuid=...
plakar at /path/to/store backup @myvm
plakar destination add pool xcpng://pool-master password=... tls_insecure=true
plakar at /path/to/store restore -to @pool <snapshot>
```

Once there are releases, pushing a `v*` tag (a semantic version, such as `v0.2.0` or
`v0.2.0-rc.1`) runs the CI checks, then builds `linux/amd64`, `linux/arm64` and
`linux/riscv64` packages with plakar 1.1.7 and attaches them to a GitHub release with a
`SHA256SUMS` file. A tag with a `-` suffix makes a pre-release. The packages are not
signed yet.

## What a backup leaves on the pool

Nothing, normally. Each backup takes a temporary snapshot named `plakar-<time>-<random>`, marked
`plakar_xcpng_snapshot` in its `other_config`, and destroys it with its disks at the end.
The importer waits up to three minutes for xapi through a toolstack restart, and a watchdog
process removes the snapshot if plakar is interrupted. A snapshot can still be left behind
if the machine running plakar loses power or the watchdog is killed, or if xapi refuses to
destroy it; the next backup of the same VM removes marked snapshots older than
`stale_snapshot_age` (default `24h`, `0` disables), and unmarked ones whose name is exactly
`plakar-<time>-<random>`, which is what a watchdog killed during the snapshot leaves. A
complete backup whose cleanup failed is kept, with a warning naming the snapshot. Details
in `docs/design.md`.

If plakar runs as a systemd service, give the watchdog time to finish when the service
stops, and leave `KillMode` at its default:

```ini
[Service]
TimeoutStopSec=11min
```

`KillMode=mixed`, `systemctl kill -s KILL`, an OOM kill of the unit and stopping a
container all kill the watchdog with plakar, so the snapshot stays until a backup of the
same VM sweeps it once it is older than `stale_snapshot_age`. To list what the connector
left on a pool:

```sh
xe snapshot-list params=uuid,name-label,snapshot-time,other-config | grep -B3 plakar_xcpng_snapshot
```

and every snapshot named like a label, marked or not, which includes the unmarked ones the
sweep also takes:

```sh
xe snapshot-list params=uuid,name-label,snapshot-time | grep -B1 -A1 -E 'name-label \( RW\): plakar-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$'
```

## What a restore leaves on the pool

A new VM with its disks, and nothing else. A restore never overwrites an existing VM. When
it starts, a restore prints its ID, `plakar-restore-<time>-<random>`. If it fails or is
interrupted, a watchdog process destroys what it had created; a disk xapi still holds is
retried, in the background once plakar has exited, for up to 10 minutes. What is left after
that (the watchdog killed, the machine powered off, or a disk held longer) is named by that
ID: VMs whose `other_config:plakar_restore` starts with it, and VDIs no VM uses whose name
or description holds it. Details in `docs/design.md`.

## Background

What the measurements found, with plakar 1.1.7 on an 8 GiB disk holding 3.4 GiB of files:

- Per-VDI raw export (`/export_raw_vdi?format=raw`) deduplicates well: in these earlier
  measurements, an unchanged second backup stores a few MB; a 184 MiB change stores about
  65 MB.
- A full XVA export (`/export`) does not deduplicate at all: every backup stores the whole
  disk again. Each export numbers its objects `Ref:N` with a different `N`, and that name
  repeats in a tar header every 1 MiB, which is the size of Plakar's content-defined chunks.
- Renumbering those ids to a fixed base restores deduplication to within about 10 % of raw,
  and the rewritten XVA still imports.
- Proxmox's VMA format has the same problem (a per-backup UUID in every extent header).

So the connector exports each VDI as raw, plus a metadata record for the VM.

## Layout

- `importer/`, `exporter/`, `internal/xapi/`, `plugin/`: the connector.

- `lab/`: the scripts used for the measurements.
  - `xexport.py`, `ximport.py`, `ximport-xva.py` run in dom0 and stream XAPI HTTP
    exports and imports through stdin/stdout, using a local XAPI session.
  - `xva-normalize.py` and `vma-normalize.py` rewrite an XVA or a VMA archive with stable
    identifiers.
  - `run-vma.sh` is one measured run of VMA against raw.

## Licence

ISC, the same as the Plakar SDK and integrations it builds on. See `LICENSE`.
