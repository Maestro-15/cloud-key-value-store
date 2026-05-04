import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


store = {}


class kvStore(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def get_key(self):
        if self.path == "/" or "/" in self.path[1:]:
            return None
        return self.path[1:]

    def send_empty_response(self, code):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        key = self.get_key()
        if key is None:
            self.send_empty_response(404)
            return

        size = int(self.headers.get("Content-Length", 0))
        value = self.rfile.read(size)
        store[key] = value

        self.send_empty_response(200)

    def do_PUT(self):
        self.do_POST()

    def do_GET(self):
        key = self.get_key()
        if key is None or key not in store:
            self.send_empty_response(404)
            return

        value = store[key]

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(value)))
        self.end_headers()
        self.wfile.write(value)

    def do_DELETE(self):
        key = self.get_key()
        if key is None:
            self.send_empty_response(404)
            return

        if key in store:
            del store[key]

        self.send_empty_response(200)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        port = int(sys.argv[1])
    else:
        port = 8000

    server = ThreadingHTTPServer(("127.0.0.1", port), kvStore)
    print("KV server running on port", port)
    server.serve_forever()
