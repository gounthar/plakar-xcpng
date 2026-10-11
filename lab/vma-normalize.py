# Rewrite a VMA archive in place: fixed UUID + ctime 0, every header md5 recomputed.
# Simulates a writer that derives the UUID from stable data instead of uuid_generate().
import sys, mmap, hashlib, struct
FIXED = bytes(16); p = sys.argv[1]
with open(p, "r+b") as f, mmap.mmap(f.fileno(), 0) as m:
    assert m[0:4] == b"VMA\0"
    old = bytes(m[8:24]); hsize = struct.unpack(">I", m[56:60])[0]
    m[8:24] = FIXED; m[24:32] = bytes(8); m[32:48] = bytes(16)
    m[32:48] = hashlib.md5(m[0:hsize]).digest()
    n, pos = 0, hsize
    while True:
        i = m.find(old, pos)
        if i < 0: break
        e = i - 8
        if m[e:e+4] == b"VMAE":
            m[i:i+16] = FIXED; m[e+24:e+40] = bytes(16)
            m[e+24:e+40] = hashlib.md5(m[e:e+512]).digest(); n += 1
        pos = i + 16
    print("extents rewritten:", n)
