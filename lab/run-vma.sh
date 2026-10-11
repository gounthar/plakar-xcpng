#!/usr/bin/env bash
# One measured run: fresh VMA archive of img.raw (vma create, same writer as vzdump), plus raw control.
set -euo pipefail
tag=$1; L=$HOME/plakar-lab/vma; P=$HOME/plakar-lab/bin/plakar
export PLAKAR_PASSPHRASE=$(cat "$L/.pass")
rm -f "$L/in-vma/disk.vma"
docker run --rm -v "$L:/w" vma-test vma create /w/in-vma/disk.vma -c /w/qemu-server.conf -d format=raw:drive-scsi0=/w/img.raw
cp "$L/in-vma/disk.vma" "$L/disk-$tag.vma"
ln -f "$L/img.raw" "$L/in-raw/disk.raw"
for f in vma raw; do
  before=$(du -sb "$L/store-$f" | cut -f1); s=$(date +%s.%N)
  "$P" at "$L/store-$f" backup -no-progress "$L/in-$f" 2>&1 | tail -1
  e=$(date +%s.%N); after=$(du -sb "$L/store-$f" | cut -f1)
  printf '%s\t%s\tinput=%s\tstored_delta=%s\tstore_total=%s\twall=%.1fs\n' "$tag" "$f" \
    "$(stat -c%s "$L/in-$f/disk.$f")" $((after-before)) "$after" "$(echo "$e-$s" | bc)" | tee -a "$L/results.tsv"
done
