# Re-stream an XVA with export-local Ref:N ids renumbered from a fixed base (Ref:900...),
# in ova.xml and in member names. Simulates an exporter that numbers refs per export, not per xapi lifetime.
import sys, re, io, tarfile
src, dst = sys.argv[1], sys.argv[2]
tin = tarfile.open(src, "r|"); tout = tarfile.open(dst, "w|", format=tarfile.USTAR_FORMAT)
remap = None
def fix(s):
    return re.sub(r"Ref:(\d+)", lambda m: "Ref:%d" % (int(m.group(1)) - remap + 900) if int(m.group(1)) >= remap else m.group(0), s)
for m in tin:
    if m.name == "ova.xml":
        x = tin.extractfile(m).read().decode()
        remap = min(int(n) for n in re.findall(r"Ref:(\d+)", x) if int(n) >= 10)
        data = fix(x).encode(); m.size = len(data); tout.addfile(m, io.BytesIO(data))
    else:
        m.name = fix(m.name); tout.addfile(m, tin.extractfile(m))
tout.close(); print(dst, "base", remap)
