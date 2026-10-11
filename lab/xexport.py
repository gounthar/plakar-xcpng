# Runs in dom0: stream an XAPI HTTP export to stdout using a local session.
import sys, shutil, XenAPI, http.client
kind, uuid = sys.argv[1], sys.argv[2]
s = XenAPI.xapi_local(); s.xenapi.login_with_password("root", "", "1.0", "plakar-lab")
sid = s._session
if kind == "raw":
    ref = s.xenapi.VDI.get_by_uuid(uuid)
    path = "/export_raw_vdi?session_id=%s&vdi=%s&format=raw" % (sid, ref)
else:
    path = "/export?session_id=%s&uuid=%s&use_compression=false" % (sid, uuid)
try:
    c = http.client.HTTPConnection("127.0.0.1", 80); c.request("GET", path); r = c.getresponse()
    if r.status != 200: sys.exit("HTTP %d" % r.status)
    shutil.copyfileobj(r, sys.stdout.buffer, 4 << 20)
finally:
    s.xenapi.session.logout()
