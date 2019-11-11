#!/usr/bin/python3.7
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
import json


class Handler(BaseHTTPRequestHandler):
    ftp_client = None

    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        args = json.loads(post_data)
        self._set_response()

        if self.path == '/init':
            self.ftp_client.initialize(args)

        self.wfile.write(json.dumps(args).encode('utf-8'))  # send message back to the sender


def test_server():
    print("Starting server...")
    http_server = HTTPServer(('127.0.0.1', 80), Handler)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
    http_server.server_close()
    print("Server is closed")


# test_server()