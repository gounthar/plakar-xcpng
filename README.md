# plakar-xcpng

Exploring a [Plakar](https://github.com/PlakarKorp/plakar) integration for XCP-ng: back up
XCP-ng VMs into a Plakar repository and restore them, with Plakar's deduplication working
across successive backups.

## Status

Exploration. Measurements are done, the connector prototype is not written yet.

What the measurements found, with plakar 1.1.7 on an 8 GiB disk holding 3.4 GiB of files:

- Per-VDI raw export (`/export_raw_vdi?format=raw`) deduplicates well: an unchanged second
  backup stores a few MB; a 184 MiB change stores about 65 MB.
- A full XVA export (`/export`) does not deduplicate at all: every backup stores the whole
  disk again. Each export numbers its objects `Ref:N` with a different `N`, and that name
  repeats in a tar header every 1 MiB, which is the size of Plakar's content-defined chunks.
- Renumbering those ids to a fixed base restores deduplication to within about 10 % of raw,
  and the rewritten XVA still imports.
- Proxmox's VMA format has the same problem (a per-backup UUID in every extent header).

So the planned connector exports each VDI as raw, plus a metadata record for the VM.

## Layout

- `lab/`: the scripts used for the measurements.
  - `xexport.py`, `ximport.py`, `ximport-xva.py` run in dom0 and stream XAPI HTTP
    exports and imports through stdin/stdout, using a local XAPI session.
  - `xva-normalize.py` and `vma-normalize.py` rewrite an XVA or a VMA archive with stable
    identifiers.
  - `run-vma.sh` is one measured run of VMA against raw.
