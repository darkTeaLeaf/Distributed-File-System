import argparse
import os
import shutil
from os.path import join, isdir, isfile

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

proto_cmds = FTPHandler.proto_cmds.copy()
proto_cmds.update(
    {'SITE RMTREE': dict(perm='d', auth=True, arg=True,
                         help='Syntax: SITE <SP> RMTREE <SP> path (remove directory tree).'),
     'SITE RMDCONT': dict(perm='d', auth=True, arg=True,
                          help='Syntax: SITE <SP> RMDCONT  (remove all nested content in the directory tree).'),
     'AVBL': dict(perm='l', auth=True, arg=True,
                  help='Syntax: AVBL (return size of total used and free disk space).'),
     }
)


class CustomizedFTPHandler(FTPHandler):
    proto_cmds = proto_cmds

    def ftp_SITE_RMTREE(self, line):
        if isdir(line):
            shutil.rmtree(line)
            self.respond("250 Directory tree was deleted successfully")
        else:
            self.respond("550 Given path is not a directory")

    def ftp_SITE_RMDCONT(self, line):
        if isdir(line):
            for obj in os.listdir(line):
                obj = join(line, obj)
                if isdir(obj):
                    shutil.rmtree(obj)
                elif isfile(obj):
                    os.remove(obj)
            self.respond("250 Content of the directory was deleted successfully")
        else:
            self.respond("550 Given path is not a directory")

    def ftp_AVBL(self, line):
        if isdir(line):
            total, used, free = shutil.disk_usage("/")
            self.respond(f"213 {total} {used} {free}")
            return total, used, free
        else:
            self.respond("550 Given path is not a directory")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--ip', type=str, required=True,
                        help='IP of the FTP server')
    args = parser.parse_args()

    authorizer = DummyAuthorizer()
    authorizer.add_user("Namenode", "1234576890", homedir="", perm="elradfmwMT")
    authorizer.add_anonymous(homedir="", perm="elr")
    # authorizer.add_user("User", "Ireksan", "", perm="elr")

    handler = CustomizedFTPHandler
    handler.authorizer = authorizer

    server = FTPServer((args.ip, 21), handler)
    server.serve_forever()
