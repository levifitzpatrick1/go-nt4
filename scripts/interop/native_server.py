#!/usr/bin/env python3
"""Separate-process real ntcore oracle. HTTP binds loopback only; no production dependency."""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import ntcore

port = int(sys.argv[1])
inst = ntcore.NetworkTableInstance.create()
inst.startServer("", "127.0.0.1", 0, port)
publishers = {}
subscribers = {}
observed = {}
received = {}
methods = {
    "boolean": "Boolean", "double": "Double", "int": "Integer", "float": "Float",
    "string": "String", "json": "String", "boolean[]": "BooleanArray",
    "double[]": "DoubleArray", "int[]": "IntegerArray", "float[]": "FloatArray",
    "string[]": "StringArray",
}


def topic(name, typ):
    suffix = methods.get(typ)
    if suffix:
        return getattr(inst, "get" + suffix + "Topic")(name)
    return inst.getRawTopic(name)


def encode_value(v):
    if not v.isValid():
        return None
    val = v.value()
    if isinstance(val, (bytes, bytearray, memoryview)):
        return list(val)
    if not isinstance(val, (bool, int, float, str, list)):
        return list(val)
    return val


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/ready":
            return self.reply(200, {"ready": True, "connected": inst.isConnected()})
        self.reply(404, {"error": "unknown path"})

    def do_POST(self):
        try:
            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            name = data.get("name", "")
            typ = data.get("type", "double")
            t = topic(name, typ) if name else None
            if self.path == "/watch":
                subscribers[name] = t.genericSubscribe(typ, ntcore.PubSubOptions(keepDuplicates=True, sendAll=True))
                return self.reply(200, {"ok": True})
            if self.path == "/publish":
                # Hold the publisher alive across HTTP requests.
                p = t.genericPublish(typ)
                publishers[name] = p
                return self.reply(200, {"ok": True})
            if self.path == "/set":
                v = data["value"]
                if typ not in methods:
                    v = bytes(v)
                suffix = methods.get(typ)
                if suffix:
                    publishers[name].set(getattr(ntcore.Value, "make" + suffix)(v))
                else:
                    publishers[name].setRaw(v)
                inst.flush()
                return self.reply(200, {"ok": True})
            if self.path == "/inspect":
                t = inst.getTopic(name)
                sub = subscribers.get(name)
                if sub:
                    for sample in sub.readQueue():
                        observed[name] = sample.value() if callable(sample.value) else sample.value
                        received.setdefault(name, []).append(list(observed[name]) if isinstance(observed[name], bytes) else observed[name])
                v = sub.get() if sub else None
                return self.reply(200, {"exists": t.exists(), "type": t.getTypeString(),
                                        "properties": t.getProperties(), "value": encode_value(v) if v else None,
                                        "queued": list(observed[name]) if name in observed and isinstance(observed[name], bytes) else observed.get(name),
                                        "received": received.get(name, []),
                                        "time": v.time() if v and v.isValid() else None,
                                        "connected": inst.isConnected()})
            if self.path == "/property":
                for k, v in data["update"].items():
                    if v is None:
                        t.deleteProperty(k)
                    else:
                        t.setProperty(k, v)
                return self.reply(200, {"ok": True})
            if self.path == "/delete":
                publishers.pop(name, None)
                return self.reply(200, {"ok": True})
            self.reply(404, {"error": "unknown path"})
        except Exception as exc:
            self.reply(500, {"error": repr(exc)})


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
try:
    server.serve_forever()
finally:
    server.server_close()
    inst.stopServer()
    inst.destroy()
