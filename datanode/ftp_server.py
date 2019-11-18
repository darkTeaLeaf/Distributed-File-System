import argparse
import os
import shutil
import subprocess
from ftplib import FTP, all_errors
from os.path import join, isdir, isfile, exists

import requests
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

proto_cmds = FTPHandler.proto_cmds.copy()
proto_cmds.update(
    {'SITE RMTREE': dict(perm='d', auth=True, arg=True,
                         help='Syntax: SITE <SP> RMTREE <SP> path (remove directory tree).'),
     'SITE RMDCONT': dict(perm='d', auth=True, arg=True,
                          help='Syntax: SITE <SP> RMDCONT (remove all nested content in the directory tree).'),
     'AVBL': dict(perm='l', auth=True, arg=True,
                  help='Syntax: AVBL (return size of total used and free disk space).'),
     'SITE EXEC': dict(perm='M', auth=True, arg=True,
                       help='Syntax: SITE <SP> EXEC line (execute given command on server).'),
     'CRF': dict(perm='a', auth=True, arg=True,
                 help='Syntax: CRF path (create empty file).'),
     'MV': dict(perm='M', auth=True, arg=True,
                help='Syntax: MV path_from path_to (move file from path_from to path_to).'),
     'REPL': dict(perm='w', auth=True, arg=True,
                  help='Syntax: REPL ip_datanode path_from path_to (make replica on ip_datanode).')
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

    def ftp_SITE_EXEC(self, line):
        process = subprocess.run(line, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        if process.returncode != 0:
            error = process.stderr.decode("utf-8")
            self.respond(f"500 {error}")
            return error
        else:
            self.respond("250 Given command was executed successfully")
            return process.stdout.decode("utf-8")

    def ftp_CRF(self, path):
        open(path, 'wb').close()
        self.respond("250 Empty file was created successfully")

    def ftp_MV(self, path_from, path_to):
        shutil.move(path_from, path_to)
        self.respond("250 File was moved successfully")

    def ftp_REPL(self, ip_datanode, path_from, path_to):
        try:
            with FTP(ip_datanode) as ftp, open(path_from, 'rb') as localfile:
                ftp.login()
                ftp.storbinary('STOR ' + path_to, localfile)
        except all_errors:
            self.respond(f"500 Replica was not created on {ip_datanode} due to connection error")
            return False

        self.respond(f"250 Replica was created on {ip_datanode}")
        return True


def connect_to_namenode(namenode_ip, homedir):
    cur_dir = os.getcwd()
    try:
        r = requests.get(f'http://{namenode_ip}:80/synchronize', json='')
        fs_tree = r.json()['msg']
        os.chdir(homedir)
        for path, dirs, files in os.walk(os.path.curdir):
            cur_tree = fs_tree
            components = path.split(os.sep)[1:]
            for c in components:
                cur_tree = cur_tree['d'][c]

            for local_dir in dirs:
                if local_dir not in cur_tree['d']:
                    shutil.rmtree(join(path, local_dir))

            for remote_dir in cur_tree['d']:
                remote_dir = join(path, remote_dir)
                if not exists(remote_dir):
                    os.mkdir(remote_dir)

            for local_file in files:
                if local_file not in cur_tree['f']:
                    os.remove(join(path, local_file))

        requests.get(f'http://{namenode_ip}:80/add_node', json='')
    except Exception as e:
        print(e)
    finally:
        os.chdir(cur_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--ip', type=str, required=True,
                        help='IP of the Datanode FTP server')
    parser.add_argument('--homedir', type=str, required=True,
                        help='Homedir of the FTP server')
    parser.add_argument('--namenode_ip', type=str, required=True,
                        help='IP of the Namenode')
    args = parser.parse_args()

    connect_to_namenode(args.namenode_ip, args.homedir)
    authorizer = DummyAuthorizer()
    authorizer.add_user("Namenode", "1234576890", homedir=args.homedir, perm="elradfmwMT")
    authorizer.add_anonymous(homedir=args.homedir, perm="elrw")

    handler = CustomizedFTPHandler
    handler.authorizer = authorizer

    server = FTPServer((args.ip, 21), handler)
    server.serve_forever()
