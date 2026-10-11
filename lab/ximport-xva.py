# Runs in dom0: stream stdin into XAPI /import (XVA) and print the new VM ref.
import sys, XenAPI, http.client
size = int(sys.argv[1]); sr = sys.argv[2]
s = XenAPI.xapi_local(); s.xenapi.login_with_password("root", "", "1.0", "plakar-lab")
try:
    sref = s.xenapi.SR.get_by_uuid(sr)
    c = http.client.HTTPConnection("127.0.0.1", 80)
    c.putrequest("PUT", "/import?session_id=%s&sr_id=%s" % (s._session, sref))
    c.putheader("Content-Length", str(size)); c.endheaders()
    left = size
    while left:
        b = sys.stdin.buffer.read(min(4 << 20, left))
        if not b: sys.exit("short input")
        c.send(b); left -= len(b)
    r = c.getresponse(); print("HTTP", r.status, r.read()[:300])
finally:
    s.xenapi.session.logout()
