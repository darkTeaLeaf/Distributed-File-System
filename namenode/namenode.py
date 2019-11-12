from collections import defaultdict
import os
import time
from multiprocessing import Process


from .ftp_client import FTPClient
from .http_handler import Handler
from http.server import HTTPServer
from .fs_tree import Directory, File


class Namenode:
    def __init__(self, address, port, num_replicas, lock_duration=300, update_time=200,
                 **auth_data):
        self.address = address
        self.port = port
        self.num_replicas = num_replicas
        self.fs_tree = Directory(None, '/')
        self.work_dir = self.fs_tree
        self.client_locks = defaultdict(dict)
        self.lock_duration = lock_duration
        self.update_time = update_time

        self.ftp_client = FTPClient(self, num_replicas, **auth_data)
        Handler.ftp_client = self.ftp_client
        self.http_server = HTTPServer((address, port), Handler)

    def start(self):
        print("Starting server on port:", self.port)
        try:
            print("Server is available on:", self.address)
            proc = Process(target=self.check_locks)
            proc.start()
            self.http_server.serve_forever()
        except KeyboardInterrupt:
            pass
        self.http_server.server_close()
        print("Server is closed")

    def check_locks(self):
        while True:
            for user_ip, locked_files in self.client_locks.items():
                for file in list(locked_files):
                    lock_start, is_write = locked_files[file]
                    if time.time() - lock_start > 300:
                        if is_write:
                            file.release_write_lock()
                        else:
                            file.release_read_lock()
                        locked_files.pop(file)
                if len(locked_files) == 0:
                    self.client_locks.pop(user_ip)
            time.sleep(self.update_time)

    def update_lock(self, client_ip, file_path):
        parent_dir, abs_path = self.work_dir.get_absolute_path(file_path)
        file_name = abs_path.split('/')[-1]
        file = parent_dir[file_name]
        lock_start, is_write = self.client_locks[client_ip][file]
        self.client_locks[client_ip][file] = (time.time(), is_write)

    def release_lock(self, client_ip, file_path):
        parent_dir, abs_path = self.work_dir.get_absolute_path(file_path)
        file_name = abs_path.split('/')[-1]
        file = parent_dir[file_name]
        lock_start, is_write = self.client_locks[client_ip].pop(file)
        if is_write:
            file.release_write_lock()
        else:
            file.release_read_lock()

        if len(self.client_locks[client_ip]) == 0:
            self.client_locks.pop(client_ip)

