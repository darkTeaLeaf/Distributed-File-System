#!/usr/bin/python3.7
from http.server import BaseHTTPRequestHandler


class Handler(BaseHTTPRequestHandler):
    ftp_client = None

    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        args = post_data.decode('utf-8').split(' ')
        self._set_response()
        response_msg = ""
        if self.path == '/init':
            response_msg = str(self.ftp_client.initialize())
        if self.path == '/create':
            response_msg = str(self.ftp_client.create_file(**args))

        self.wfile.write(response_msg.encode("utf-8"))  # send message back to the sender



