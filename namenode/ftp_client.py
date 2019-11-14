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
        abs_path = os.path.join(str(parent_dir), file_name)
        return parent_dir, abs_path, file_name

    def get_dir(self, file_path):
        parent_dir, abs_path = self.namenode.work_dir.get_absolute_path(file_path)
        if parent_dir is None:
            return None, 'Incorrect path', None

        if str(parent_dir) == abs_path:
            return parent_dir, str(parent_dir), str(parent_dir)
        else:
            file_name = file_path.split('/')[-1]
            abs_path = os.path.join(str(parent_dir), file_name)
            return parent_dir, abs_path, file_name

    def create_file(self, file_path):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        if file_name in parent_dir:
            return 'File already exists.'

        file = parent_dir.add_file(file_name)
        file.set_write_lock()
        try:
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
            file.release_write_lock()
        except Exception as e:
            file.release_write_lock()
            parent_dir.delete_file(file_name)
            return 'File was not created due to internal error.'
        return ''

    def read_file(self, file_path, client_ip):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        if file_name not in parent_dir:
            return 'File does not exist.'

        file = parent_dir.children_files[file_name]
        if not file.readable():
            return 'File is being written. Reading cannot be performed.'

        self.namenode.set_client_lock(client_ip, file, 0)
        return {'ips': list(file.nodes), 'path': abs_path}

    def write_file(self, file_path, client_ip, file_size):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        old_file_size = 0
        if file_name in parent_dir:
            file = parent_dir.children_files[file_name]
            if not file.writable():
                return 'File is blocked by another process. Writing cannot be performed.'
            for datanode in file.nodes:
                try:
                    with FTP(datanode, **self.auth_data) as ftp:
                        ftp.voidcmd('TYPE I')
                        old_file_size = ftp.size(str(file))
                        break
                except ConnectionRefusedError:
                    continue

        else:
            file = parent_dir.add_file(file_name)
        self.namenode.set_client_lock(client_ip, file, 1)

        selected_datanodes = set()
        for datanode in self.datanodes:
            try:
                with FTP(datanode, **self.auth_data) as ftp:
                    if ftp.sendcmd("AVBL /").split(' ')[3] - old_file_size > file_size:
                        selected_datanodes.add(datanode)
            except ConnectionRefusedError:
                continue
        file.nodes = selected_datanodes
        if len(selected_datanodes) == 0:
            self.namenode.release_lock(client_ip, abs_path)

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

        return 'File was deleted'

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

    def create_directory(self, dir_path):
        parent_dir, abs_path, dir_name = self.get_dir(dir_path)

        if parent_dir is None:
            return abs_path

        if dir_name in parent_dir or abs_path == str(parent_dir):
            return 'Directory already exist.'

        try:
            dir = parent_dir.add_directory(dir_name)
            dir.set_write_lock()

            for datanode in self.datanodes:
                try:
                    with FTP(datanode, **self.auth_data) as ftp:
                        ftp.voidcmd(f"MKD {abs_path}")
                except ConnectionRefusedError:
                    continue
        except Exception as e:
            parent_dir.delete_directory(dir_name)
            return 'Directory was not created due to internal error.'
        finally:
            dir.release_write_lock()
        return ''

    def open_directory(self, dir_path):
        parent_dir, abs_path, dir_name = self.get_dir(dir_path)
        if parent_dir is None:
            return abs_path

        self.namenode.work_dir.release_read_lock()
        if str(parent_dir) != abs_path:
            self.namenode.work_dir = parent_dir.children_directories[dir_name]
        else:
            self.namenode.work_dir = parent_dir
        self.namenode.work_dir.set_read_lock()

        return ''

    def delete_directory(self, dir_path, force_delete=False):
        parent_dir, abs_path, dir_name = self.get_file(dir_path)

        if parent_dir is None:
            return abs_path
        if abs_path == str(parent_dir):
            return 'You cannot delete root directory.', 0

        if dir_name not in parent_dir:
            return 'Directory does not exist.', 0

        dir = parent_dir.children_directories[dir_name]
        if not dir.writable():
            return 'Directory is blocked by another process. Deleting cannot be performed.', 0

        if (dir.children_directories or dir.children_files) and not force_delete:
            return 'Directory is not empty. Are you sure to delete it anyway?[Y/n]', 1

        parent_dir.delete_directory(dir_name)

        for datanode in self.datanodes:
            try:
                with FTP(datanode, **self.auth_data) as ftp:
                    ftp.voidcmd(f"SITE RMTREE {abs_path}")
            except ConnectionRefusedError:
                continue

        return 'Directory was deleted', 0

    def read_directory(self, dir_path=None):
        if dir_path is None:
            parent_dir, abs_path, dir_name = self.get_dir(str(self.namenode.work_dir))
        else:
            parent_dir, abs_path, dir_name = self.get_dir(dir_path)

        if parent_dir is None:
            return abs_path

        if str(parent_dir) != abs_path:
            if dir_name not in parent_dir:
                return 'Directory does not exist.'
            dir = parent_dir.children_directories[dir_name]
        else:
            dir = parent_dir
        files = [name for name, obj in dir.children_files.items()]
        dirs = [name for name, obj in dir.children_directories.items()]

        return {'files': files, 'dirs': dirs}

    def move_file(self, path_from, path_to):
        file_parent_dir, file_abs_path, file_name = self.get_file(path_from)
        dir_parent_dir, dir_abs_path, dir_name = self.get_dir(path_to)

        if file_parent_dir or dir_parent_dir is None:
            return file_abs_path, dir_abs_path

        if file_name not in file_parent_dir:
            return 'File does not exist.'

        if dir_name not in dir_parent_dir:
            return 'Directory does not exist.'

        file = file_parent_dir.children_files[file_name]
        dir = dir_parent_dir.children_directories[dir_name]
        file.set_write_lock()

        if file_name in dir_parent_dir.children_files:
            return 'File with the same name already exist in directory.'

        new_file = dir.add_file(file_name)
        new_file.set_write_lock()

        try:
            for datanode in file.nodes:
                try:
                    with FTP(datanode, **self.auth_data) as ftp:
                        ftp.voidcmd(f"MV {file_abs_path} {dir_abs_path}")
                except ConnectionRefusedError:
                    continue
            file.release_write_lock()
            file_parent_dir.delete_file(file_name)
            new_file.release_write_lock()
        except Exception as e:
            new_file.release_write_lock()
            dir.delete_file(file_name)
            file.release_write_lock()
            return 'File was not moved due to internal error.'
        return ''
