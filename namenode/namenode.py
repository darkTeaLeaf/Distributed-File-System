import argparse
import time
from collections import defaultdict
from http.server import HTTPServer
from threading import Thread

from namenode.fs_tree import Directory
from namenode.ftp_client import FTPClient
from namenode.http_handler import Handler


def check_locks(update_time, client_locks, lock_duration):
    while True:
        dropped_list = []
        for user_ip, locked_files in client_locks.items():
            for file in list(locked_files):
                lock_start, is_write = locked_files[file]
                if time.time() - lock_start > lock_duration:
                    if is_write:
                        file.release_write_lock()
                    else:
                        file.release_read_lock()
                    locked_files.pop(file)
            if len(locked_files) == 0:
                dropped_list.append(user_ip)
        for user_ip in dropped_list:
            client_locks.pop(user_ip)
        time.sleep(update_time)
        print('Update')


class Namenode:
    def __init__(self, address, port, num_replicas, lock_duration=300, update_time=200,
                 **auth_data):
        self.address = address
        self.port = port
        self.num_replicas = num_replicas
        self.fs_tree = Directory('/')
        self.fs_tree.set_read_lock()
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
            t = Thread(target=check_locks, args=(self.update_time, self.client_locks, self.lock_duration))
            t.start()
            self.http_server.serve_forever()
        except KeyboardInterrupt:
            pass
        self.http_server.server_close()
        print("Server is closed")

    def traverse_fs_tree(self):
        return self.fs_tree.to_dict()

    def add_datanode(self, datanode_ip):
        if datanode_ip == '127.0.0.1':
            datanode_ip = 'localhost'
        self.ftp_client.datanodes.add(datanode_ip)
        return ''

    def set_lock(self, client_ip, file, is_write=False):
        if is_write:
            file.set_write_lock()
        else:
            file.set_read_lock()

        self.client_locks[client_ip][file] = (time.time(), is_write)

    def update_lock(self, client_ip, file_path):
        parent_dir, abs_path = self.work_dir.get_absolute_path(file_path)
        file_name = file_path.split('/')[-1]
        file = parent_dir.children_files[file_name]
        lock_start, is_write = self.client_locks[client_ip][file]
        self.client_locks[client_ip][file] = (time.time(), is_write)

    def release_lock(self, client_ip, file_path):
        parent_dir, abs_path = self.work_dir.get_absolute_path(file_path)
        file_name = file_path.split('/')[-1]
        file = parent_dir.children_files[file_name]
        lock_start, is_write = self.client_locks[client_ip].pop(file)
        if is_write:
            file.release_write_lock()
        else:
            file.release_read_lock()

        if len(self.client_locks[client_ip]) == 0:
            self.client_locks.pop(client_ip)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--ip', type=str, required=True,
                        help='IP of the Namenode')
    parser.add_argument('--lock_duration', type=int, required=True,
                        help='Duration of the lock for read or written files')
    parser.add_argument('--update_time', type=int, required=True,
                        help='Period of checking locks in files')
    parser.add_argument('--num_replicas', type=int, required=True,
                        help='Number of the replicas in the file system')
    args = parser.parse_args()

    node = Namenode(args.ip, 80, args.num_replicas, lock_duration=args.lock_duration, update_time=args.update_time,
                    user="Namenode", passwd="1234576890")

    node.start()
