from .ftp_client import FTPClient
from .http_handler import Handler
from http.server import HTTPServer
from .fs_tree import Directory, File


class Namenode:
    def __init__(self, address, port, num_replicas, **auth_data):
        self.address = address
        self.port = port
        self.num_replicas = num_replicas
        self.fs_tree = Directory(None, '/')
        self.curdir = '/'

        self.ftp_client = FTPClient(self, num_replicas, **auth_data)
        Handler.ftp_client = self.ftp_client
        self.http_server = HTTPServer((address, port), Handler)

    def start(self):
        print("Starting server on port:", self.port)
        try:
            print("Server is available on:", self.address)
            self.http_server.serve_forever()
        except KeyboardInterrupt:
            pass
        self.http_server.server_close()
        print("Server is closed")