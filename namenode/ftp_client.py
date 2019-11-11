from ftplib import FTP

from .fs_tree import Directory, File


class FTPClient:
    def __init__(self, namenode, num_replicas, **auth_data):
        self.num_replicas = num_replicas
        self.namenode = namenode
        self.datanodes = set()

        self.ftp = FTP()
        self.auth_data = auth_data

    def initialize(self):
        disk_sizes = []
        for datanode in self.datanodes:
            self.ftp.connect(datanode, 21)
            self.ftp.login(**self.auth_data)
            self.ftp.voidcmd("SITE RMDCONT /")
            available_size = self.ftp.sendcmd("AVBL /").split(' ')[1]
            disk_sizes.append(available_size)
            self.ftp.quit()

            self.namenode.fs_tree = Directory(None, '/')
            self.namenode.curdir = '/'
        return sum(disk_sizes)
