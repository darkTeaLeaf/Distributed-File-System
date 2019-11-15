import os
from datetime import datetime
from ftplib import FTP
from threading import Thread

from namenode.fs_tree import Directory, File


def create_replica(status, source_ip, dest_ip, path_from, path_to, auth_data):
    try:
        with FTP(source_ip, **auth_data) as ftp:
            responce = ftp.sendcmd(f"REPL {dest_ip} {path_from} {path_to}").split(' ')[0]
            status[dest_ip] = responce == '250'
    except ConnectionRefusedError:
        status[dest_ip] = False


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

        self.namenode.set_lock(client_ip, file, 0)
        return {'ips': list(file.nodes), 'path': abs_path}

    def write_file(self, file_path, client_ip, file_size):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path

        if file_name in parent_dir:
            file = parent_dir.children_files[file_name]
            if not file.writable():
                return 'File is blocked by another process. Writing cannot be performed.'
        else:
            file = parent_dir.add_file(file_name)
        self.namenode.set_lock(client_ip, file, 1)

        selected_nodes = self._select_datanodes_for_write(file, file_size)
        if len(selected_nodes) == 0:
            self.namenode.release_lock(client_ip, abs_path)
            return 'There is no available nodes to store file'
        else:
            file.new_nodes = selected_nodes
            return {'ips': list(selected_nodes), 'path': abs_path}

    def replicate_file(self, file_path, client_ip, node_ip):
        parent_dir, abs_path, file_name = self.get_file(file_path)
        if parent_dir is None:
            return abs_path
        file = parent_dir.children_files[file_name]
        self.namenode.release_lock(client_ip, abs_path)
        file.set_write_lock()
        if node_ip in file.nodes:
            file.nodes.remove(node_ip)
        self._delete_file_from_nodes(file)
        file.nodes = file.new_nodes
        left_nodes = file.nodes.copy()

        storing_nodes = {node_ip}
        while len(left_nodes) > 0 and len(storing_nodes) < self.num_replicas:
            statuses = {}
            i = len(storing_nodes)
            threads = []
            for storing_node, left_node in zip(storing_nodes, left_nodes):
                if i >= self.num_replicas:
                    break
                args = (statuses, storing_node, left_node, str(file), str(file), self.auth_data)
                thread = Thread(target=create_replica, args=args)
                thread.start()
                threads.append((left_node, storing_node, thread))
                i += 1

            for thread in threads:
                thread[-1].join()

            for (dest_node, status), (_, source_node, _) in zip(sorted(statuses.items()), sorted(threads)):
                left_nodes.remove(dest_node)
                if status:
                    storing_nodes.add(dest_node)
        file.nodes = storing_nodes
        return "File was replicated"

    def _select_datanodes_for_write(self, file, file_size):
        selected_nodes = set()
        for node in self.datanodes:
            try:
                with FTP(node, **self.auth_data) as ftp:
                    old_file_size = file.size if node in file.nodes else 0
                    if int(ftp.sendcmd("AVBL /").split(' ')[3]) - old_file_size > file_size:
                        selected_nodes.add(node)
            except ConnectionRefusedError:
                continue
        return selected_nodes

    def _delete_file_from_nodes(self, file):
        for datanode in file.nodes:
            try:
                with FTP(datanode, **self.auth_data) as ftp:
                    ftp.voidcmd(f"DELE {file}")
            except ConnectionRefusedError:
                continue

    def _copy_file_on_nodes(self, file, new_file):
        for node in file.nodes:
            try:
                with FTP(node, **self.auth_data) as ftp:
                    ftp.voidcmd(f"MV {file}, {new_file}")
            except ConnectionRefusedError:
                continue

    def copy_file(self, file_path_old, file_path_new):
        parent_dir_old, abs_path_old, file_name_old = self.get_file(file_path_old)
        parent_dir_new, abs_path_new, file_name_new = self.get_file(file_path_new)
        if parent_dir_old is None or parent_dir_new is None:
            return abs_path_old

        if file_name_old not in parent_dir_old:
            return 'Source file does not exist.'
        elif parent_dir_new in parent_dir_new:
            return 'Destination already exist. Delete or move to another location.'

        file_old: File = parent_dir_old.children_files[file_name_old]
        file_new = parent_dir_new.add_file(file_name_new)
        file_new.set_write_lock()

        if not file_old.readable():
            return 'File is being written. Copying cannot be performed.'
        file_old.set_read_lock()
        self._copy_file_on_nodes(file_old, file_new)
        return 'File has been copied successfully.'

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
        self._delete_file_from_nodes(file)
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
            new_dir = parent_dir.add_directory(dir_name)
            new_dir.set_write_lock()

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
            new_dir.release_write_lock()
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

        if dir_parent_dir is None:
            return dir_abs_path

        if file_parent_dir is None:
            return file_abs_path

        if file_name not in file_parent_dir:
            return 'File does not exist.'

        if str(dir_parent_dir) != dir_abs_path:
            if dir_name not in dir_parent_dir:
                return 'Directory does not exist.'
            dir = dir_parent_dir.children_directories[dir_name]
        else:
            dir = dir_parent_dir

        file = file_parent_dir.children_files[file_name]
        file.set_write_lock()

        if file_name in dir_parent_dir.children_files:
            return 'File with the same name already exist in directory.'

        new_file = dir.add_file(file_name)
        new_file.set_write_lock()

        try:
            for datanode in file.nodes:
                try:
                    with FTP(datanode, **self.auth_data) as ftp:
                        ftp.voidcmd(f"MV {file} {dir}")
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
