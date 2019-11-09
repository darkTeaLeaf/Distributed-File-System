#!/usr/bin/python3.7
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        name, psw = post_data.decode('utf-8').split(',')
        self._set_response()
        response_msg = ""
        if self.path == '/login':
            print(name, psw)
            response_msg = "LOGIN SUCCESS!"
        self.wfile.write(response_msg.encode("utf-8"))  # send message back to the sender


def start(addr, port):
    print("Starting server on port:", port)
    httpd = HTTPServer((addr, port), Handler)
    try:
        print("Server is available on:", addr)
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

start('127.0.0.1', 80)
