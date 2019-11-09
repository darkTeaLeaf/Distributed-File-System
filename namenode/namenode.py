from ftplib import FTP

from .fs_tree import Directory, File


class Namenode:
    def __init__(self, num_replicas, **auth_data):
        self.num_replicas = num_replicas
        self.fs_tree = Directory(None, '/')
        self.datanodes = set()
        self.curdir = '/'

        self.ftp = FTP()
        self.auth_data = auth_data

    def listener(self, command, args):
        if command == 'init':
            responce = self.initialize()

        return responce

    def initialize(self):
        disk_sizes = []
        for datanode in self.datanodes:
            self.ftp.connect(datanode, 21)
            self.ftp.login(**self.auth_data)
            self.ftp.voidcmd("SITE RMDCONT /")
            available_size = self.ftp.sendcmd("AVBL /").split(' ')[1]
            disk_sizes.append(available_size)
            self.ftp.quit()

            self.fs_tree = Directory(None, '/')
            self.curdir = '/'
        return sum(disk_sizes)
