#!/usr/bin/python3.7
import json
from http.server import BaseHTTPRequestHandler

from .ftp_client import FTPClient


class Handler(BaseHTTPRequestHandler):
    ftp_client: FTPClient = None

    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def _form_message(self, args):
        msg = {'msg': 'success'}

        if self.path == '/synchronize':
            msg['msg'] = self.ftp_client.namenode.traverse_fs_tree()
        elif self.path == '/add_node':
            msg['msg'] = self.ftp_client.namenode.add_datanode(self.client_address[0])
        elif self.path == '/init':
            msg['msg'] = self.ftp_client.initialize()
        elif self.path == '/create':
            msg['msg'] = self.ftp_client.create_file(**args)
        elif self.path == '/read':
            msg['msg'] = self.ftp_client.read_file(client_ip=self.client_address[0], **args)
        elif self.path == '/update_lock':
            msg['msg'] = self.ftp_client.namenode.update_lock(client_ip=self.client_address[0], **args)
        elif self.path == '/release_lock':
            msg['msg'] = self.ftp_client.namenode.release_lock(client_ip=self.client_address[0], **args)
        elif self.path == '/rm':
            msg['msg'] = self.ftp_client.remove_file(**args)
        elif self.path == '/info':
            msg['msg'] = self.ftp_client.get_info(**args)
        elif self.path == '/mkdir':
            msg['msg'] = self.ftp_client.create_directory(**args)
        elif self.path == '/cd':
            msg['msg'] = self.ftp_client.open_directory(**args)
        elif self.path == '/ls':
            msg['msg'] = self.ftp_client.read_directory(**args)
        elif self.path == '/rmdir':
            msg['msg'] = self.ftp_client.delete_directory(**args)
        elif self.path == '/write':
            msg['msg'] = self.ftp_client.write_file(client_ip=self.client_address[0], **args)
        elif self.path == '/replicate_file':
            msg['msg'] = self.ftp_client.replicate_file(client_ip=self.client_address[0], **args)
        elif self.path == '/copy':
            pass
        elif self.path == '/move':
            msg['msg'] = self.ftp_client.move_file(**args)
        else:
            print("Error! Command doesn't exist.")
            msg['msg'] = 'failure'

        return msg

    def do_GET(self):
        content_length = int(self.headers['Content-Length'])

        post_data = self.rfile.read(content_length)
        args = json.loads(post_data)

        try:
            msg = self._form_message(args)
            self._set_response()
            self.wfile.write(json.dumps(msg).encode('utf-8'))  # send message back to the sender
        except ConnectionResetError:
            pass
