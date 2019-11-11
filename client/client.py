#!/usr/bin/python3.7
from ftplib import FTP
import requests
import sys


NAMENODE_ADDR = 'ireknazm.space'


def connect(addr):
    ftp = FTP(addr)
    ftp.connect('localhost', 1026)
    ftp.login()
    ftp.cwd('directory_name')       # replace with your directory
    ftp.retrlines('LIST')


def upload_file(ftp, filename):
    ftp.storbinary('STOR '+filename, open(filename, 'rb'))
    ftp.quit()


def download_file(ftp, filename):
    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    ftp.quit()
    localfile.close()


def start_http_server():
    pass


def login(*args):
    r = requests.get('http://127.0.0.1/login', data=' '.join(list(map(str, args))))
    print(r.text)


def main():
    pass


if __name__ == "__main__":
    # main()
    args = sys.argv[1:]
    if args[0] == 'login':
        login(args[1], args[2])
