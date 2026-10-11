# Runs in dom0: stream stdin into a VDI via XAPI /import_raw_vdi.
import sys, XenAPI, http.client
uuid, size = sys.argv[1], int(sys.argv[2])
s = XenAPI.xapi_local(); s.xenapi.login_with_password("root", "", "1.0", "plakar-lab")
try:
    ref = s.xenapi.VDI.get_by_uuid(uuid)
    c = http.client.HTTPConnection("127.0.0.1", 80)
    c.putrequest("PUT", "/import_raw_vdi?session_id=%s&vdi=%s&format=raw" % (s._session, ref))
    c.putheader("Content-Length", str(size)); c.endheaders()
    left = size
    while left:
        b = sys.stdin.buffer.read(min(4 << 20, left))
        if not b: sys.exit("short input")
        c.send(b); left -= len(b)
    r = c.getresponse(); print("HTTP", r.status, r.read()[:200])
finally:
    s.xenapi.session.logout()
