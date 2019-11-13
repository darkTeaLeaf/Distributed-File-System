#!/usr/bin/python3.7
from ftplib import FTP, all_errors
import requests
import sys
import subprocess
import re
import time
from threading import Thread, Event


NAMENODE_ADDR = 'ireknazm.space'


def download_file(ftp, filename):
    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    ftp.quit()
    localfile.close()


def print_help():
    print("""\nList of available commands:
    init
    create <file name/path in FS>
    read   <file name/path in FS> <file name/path on Client>
    write  <file name/path on Client> <file name/path in FS>
    rm     <file name/path in FS>
    info   <file name/path in FS>
    copy   <from file name/path in FS> <to file name/path in FS>
    move   <from file name/path in FS> <to file name/path in FS>
    cd     <folder name/path in FS>
    ls     <folder name/path in FS>
    mkdir  <folder name/path in FS>
    rmdir  <folder name/path in FS>\n""")


def send_req(cmd, args=''):
    try:
        r = requests.get('http://127.0.0.1:80/'+cmd, json=args)
        return  r.json()['msg']
    except Exception as e:
        print(e)


def ping_datanodes(datanodes):
    latency = []
    for datanode in datanodes:
        ping = subprocess.Popen(["ping", datanode, "-n", "1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                shell=True)
        output = ping.communicate()

        pattern = r"Average = (\d+\S+)"
        latency.append(int(re.findall(pattern, output[0].decode())[0][:-2]))

    return latency


def ftp_read(event, filename, datanodes, **auth_data):
    latency = ping_datanodes(datanodes)
    data_stored = False

    for latency, datanode in sorted(zip(latency, datanodes)):
        try:
            ftp = FTP(datanode, **auth_data)
            ftp.storbinary('STOR ' + filename, open(filename, 'rb'))
            ftp.quit()
            data_stored = True
            break
        except all_errors:
            continue

    if not data_stored:
        print('Cannot connect to datanode')

    event.set()


def update_lock(event, file_from):
    while not event.wait(300):
        send_req('update_lock', {'path_from': file_from})


def read_file(file_from, file_to, **auth_data):
    datanodes = send_req('read', {'path_from': file_from})

    event = Event()
    read_file_ftp = Thread(target=ftp_read, args=(event, file_to, datanodes, auth_data))
    send_clock_update = Thread(target=update_lock, args=(event, file_from))

    read_file_ftp.start()
    send_clock_update.start()

    read_file_ftp.join()
    send_clock_update.join()

    send_req('release_lock', {'path_from': file_from})


def main():
    args = sys.argv[1:]     # get command with arguments
    if len(args) == 0:
        print("Empty command!\nFor help write command: help")
    elif len(args) == 1:            # commands without any argument
        if args[0] == 'help':
            print_help()
        elif args[0] == 'init':
            send_req('init')
        else:
            print("Incorrect command!\nFor help write command: help")
    elif len(args) == 2:            # commands with 1 argument
        if args[0] == 'create':
            send_req('create', {'file_path': args[1]})
        elif args[0] == 'rm':
            send_req('rm', {'file_path': args[1]})
        elif args[0] == 'info':
            send_req('info', {'file_path': args[1]})
        elif args[0] == 'cd':
            send_req('cd', {'path': args[1]})
        elif args[0] == 'ls':
            send_req('ls', {'path': args[1]})
        elif args[0] == 'mkdir':
            send_req('mkdir', {'path': args[1]})
        elif args[0] == 'rmdir':
            send_req('rmdir', {'path': args[1]})
        else:
            print("Incorrect command!\nFor help write command: help")
    elif len(args) == 3:            # commands with 2 arguments
        if args[0] == 'login':
            send_req('login', {'username': args[1], 'password': args[2]})
        elif args[0] == 'read':
            read_file(args[1], args[2])
        elif args[0] == 'write':
            send_req('write', {'path_from': args[1], 'path_to': args[2]})
        elif args[0] == 'copy':
            send_req('copy', {'path_from': args[1], 'path_to': args[2]})
        elif args[0] == 'move':
            send_req('move', {'path_from': args[1], 'path_to': args[2]})
        else:
            print("Incorrect command!\nFor help write command: help")
    else:
        print("Wrong amount of arguments!\nFor help write command: help")


if __name__ == "__main__":
    main()
