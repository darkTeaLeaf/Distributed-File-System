import os
import time
from ftplib import FTP
from datetime import datetime

from namenode.fs_tree import Directory, File


class FTPClient:
    def __init__(self, namenode, num_replicas, **auth_data):
        self.num_replicas = num_replicas
        self.namenode = namenode
        self.datanodes = set()

        self.auth_data = auth_data

    def initialize(self):
        disk_sizes = []
        for datanode in self.datanodes:
            try:
                with FTP(datanode, **self.auth_data) as ftp:
                    ftp.voidcmd("SITE RMDCONT /")
                    available_size = ftp.sendcmd("AVBL /").split(' ')[3]
                    disk_sizes.append(int(available_size))
            except ConnectionRefusedError:
                continue

            self.namenode.fs_tree = Directory('/')
            self.namenode.work_dir = self.namenode.fs_tree
        result = sum(disk_sizes)
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        i = 0
        while result / 1000 > 2:
            i += 1
            result /= 1000
        return f"Available size of the storage is {round(result, 2)} {units[i]}"

    def get_file(self, file_path):
        parent_dir, abs_path = self.namenode.work_dir.get_absolute_path(file_path)
        if parent_dir is None:
            return None, 'Incorrect path', None

        file_name = file_path.split('/')[-1]
        abs_path = os.path.join(abs_path, file_name)
        return parent_dir, abs_path, file_name

    def create_file(self, file_path):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        if file_name in parent_dir:
            return 'File already exists.'

        try:
            file = parent_dir.add_file(file_name)
            file.set_write_lock()

            selected_datanodes = set()
            for datanode in self.datanodes:
                if len(selected_datanodes) > self.num_replicas:
                    break
                try:
                    with FTP(datanode, **self.auth_data) as ftp:
                        ftp.voidcmd(f"CRF {abs_path}")
                        selected_datanodes.add(datanode)
                except ConnectionRefusedError:
                    continue
            file.nodes = selected_datanodes
        except Exception as e:
            parent_dir.delete_file(file_name)
            return 'File was not created due to internal error.'
        finally:
            file.release_write_lock()
        return ''

    def read_file(self, file_path, client_ip):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        if file_name not in parent_dir:
            return 'File does not exist.'

        file = parent_dir[file_name]
        if not file.readable():
            return 'File is being written. Reading cannot be performed.'

        file.set_read_lock()
        self.namenode.client_locks[client_ip][file] = (time.time(), 0)
        return {'ips': list(file.nodes), 'path': abs_path}

    def remove_file(self, file_path):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        if file_name not in parent_dir:
            return 'File does not exist.'

        file = parent_dir.children_files[file_name]
        if not file.writable():
            return 'File is blocked by another process. Deleting cannot be performed.'

        parent_dir.delete_file(file_name)

        for datanode in self.datanodes:
            try:
                with FTP(datanode, **self.auth_data) as ftp:
                    ftp.voidcmd(f"DELE {file}")
            except ConnectionRefusedError:
                continue

        return 'File was deteled'

    def get_info(self, file_path):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        if file_name not in parent_dir:
            return 'File does not exist.'

        file = parent_dir.children_files[file_name]
        if not file.readable():
            return 'File is being written. Reading cannot be performed.'

        file.set_read_lock()

        result = 'File is not accessed'
        for datanode in file.nodes:
            try:
                with FTP(datanode, **self.auth_data) as ftp:
                    ftp.voidcmd('TYPE I')
                    size = ftp.size(str(file))
                    date = ftp.sendcmd(f"MDTM {file}").split(' ')[1]
                    date = datetime.strptime(date, "%Y%m%d%H%M%S").isoformat(' ')
                    details = ftp.sendcmd(f"LIST {file}").split(' ', 1)[1]

                    units = ['B', 'KB', 'MB', 'GB', 'TB']
                    i = 0
                    while size / 1000 > 2:
                        i += 1
                        size /= 1000
                    result = f"Size of the file is {round(size, 2)} {units[i]}"
                    result += f'\nLast modified: {date}\n{details}'
                break
            except ConnectionRefusedError:
                continue
        return result
