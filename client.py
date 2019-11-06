#!/usr/bin/python3.7
from ftplib import FTP
import argparse


DOMAIN_NAME = 'ireknazm.space'


def connect():
    ftp = FTP(DOMAIN_NAME)
    ftp.connect('localhost', 1026)
    ftp.login()
    ftp.cwd('directory_name')       # replace with your directory
    ftp.retrlines('LIST')


def uploadFile(ftp, filename):
    ftp.storbinary('STOR '+filename, open(filename, 'rb'))
    ftp.quit()


def downloadFile(ftp, filename):
    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    ftp.quit()
    localfile.close()


def main():
    print("HELLO!")


if __name__ == "__main__":
    main()
