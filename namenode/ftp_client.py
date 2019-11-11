import os
from ftplib import FTP

from .fs_tree import Directory, File


class FTPClient:
    def __init__(self, namenode, num_replicas, **auth_data):
        self.num_replicas = num_replicas
        self.namenode = namenode
        self.datanodes = set()

        self.auth_data = auth_data

    def initialize(self):
        disk_sizes = []
        for datanode in self.datanodes:
            with FTP(datanode, **self.auth_data) as ftp:
                ftp.voidcmd("SITE RMDCONT /")
                available_size = ftp.sendcmd("AVBL /").split(' ')[1]
                disk_sizes.append(available_size)

            self.namenode.fs_tree = Directory('/')
            self.namenode.work_dir = self.namenode.fs_tree
        return sum(disk_sizes)

    def create_file(self, file_path):
        parent_dir, abs_path = self.namenode.work_dir.get_absolute_path(file_path)
        if parent_dir is None:
            return abs_path

        file_name = abs_path.split('/')[-1]
        if file_name in parent_dir:
            return 'File already exists.'

        try:
            file = parent_dir.add_file(file_name)
            file.set_write_lock()

            selected_datanodes = []
            for datanode in self.datanodes:
                if len(selected_datanodes) > self.num_replicas:
                    continue

                with FTP(datanode, **self.auth_data) as ftp:
                    ftp.voidcmd(f"CRF {abs_path}")
                    selected_datanodes.append(datanode)
            file.nodes = selected_datanodes
        except Exception as e:
            parent_dir.pop(file_name)
            return 'File was not created due to internal error.'
        finally:
            file.release_write_lock()
        return ''

    def read_file(self, file_path):
        parent_dir, abs_path = self.namenode.work_dir.get_absolute_path(file_path)
        if parent_dir is None:
            return abs_path

        file_name = abs_path.split('/')[-1]
        if file_name not in parent_dir:
            return 'File does not exist.'

        file = parent_dir[file_name]
        if not file.readable():
            return 'File is being written. Reading cannot be performed.'

        file.set_read_lock()
        return ' '.join(file.nodes)



