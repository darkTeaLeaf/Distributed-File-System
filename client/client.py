#!/usr/bin/python3.7
import sys
from ftplib import FTP, all_errors
from threading import Thread, Event
import os
import requests
from tcp_latency import measure_latency

NAMENODE_ADDR = 'ireknazm.space'


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
        r = requests.get('http://127.0.0.1:80/' + cmd, json=args)
        print(r.json()['msg'])
        return r.json()['msg']
    except Exception as e:
        print(e)


def ping_datanodes(datanodes):
    latency = []
    for datanode in datanodes:
        latency.append(measure_latency(host=datanode, port=21)[0])
    return latency


def update_lock(event, file_from):
    while not event.wait(300):
        send_req('update_lock', {'file_path': file_from})


def read_file(file_from, file_to):
    result = send_req('read', {'file_path': file_from})
    if isinstance(result, 'str'):
        print(result)
        return
    datanodes = result['ips']
    file_from = result['path']

    event = Event()
    send_clock_update = Thread(target=update_lock, args=(event, file_from))
    send_clock_update.start()

    latency = ping_datanodes(datanodes)
    data_stored = False
    for latency, datanode in sorted(zip(latency, datanodes)):
        try:
            with FTP(datanode) as ftp, open(file_to, 'wb') as localfile:
                ftp.login()
                ftp.retrbinary('RETR ' + file_from, localfile.write, 1024)
                data_stored = True
                break
        except all_errors:
            continue

    if not data_stored:
        print('Cannot connect to datanode')

    event.set()
    send_clock_update.join()

    send_req('release_lock', {'file_path': file_from})


def write_file(file_from, file_to=None):
    if file_to is None:
        file_to = file_from
    result = send_req('write', {'file_path': file_to, 'file_size': os.path.getsize(file_from)})
    if isinstance(result, str):
        print(result)
        return

    datanodes = result['ips']
    file_to = result['path']

    event = Event()
    send_clock_update = Thread(target=update_lock, args=(event, file_to))
    send_clock_update.start()

    latencies = ping_datanodes(datanodes)

    connected_node = None
    for latency, datanode in sorted(zip(latencies, datanodes)):
        try:
            with FTP(datanode) as ftp, open(file_from, 'rb') as localfile:
                ftp.login()
                ftp.storbinary('STOR ' + file_to, localfile)
                connected_node = datanode
            break
        except all_errors:
            continue

    if connected_node is None:
        print('Cannot connect to datanode')
        send_req('release_lock', {'file_path': file_to})
    else:
        t = Thread(target=send_req, daemon=True,
                   args=('replicate_file', {'file_path': file_to, 'node_ip': connected_node}))
        t.start()

    event.set()
    send_clock_update.join()


def delete_directory(dir_path):
    response, flag = send_req('rmdir', {'dir_path': dir_path})

    if flag:
        answer = input(response)
        if answer == 'Y':
            send_req('rmdir', {'dir_path': dir_path, 'force_delete': True})
    else:
        print(response)


def main():
    args = sys.argv[1:]  # get command with arguments
    if len(args) == 0:
        print("Empty command!\nFor help write command: help")
    elif len(args) == 1:  # commands without any argument
        if args[0] == 'help':
            print_help()
        elif args[0] == 'init':
            send_req('init')
        elif args[0] == 'ls':
            send_req('ls', {})
        else:
            print("Incorrect command!\nFor help write command: help")
    elif len(args) == 2:  # commands with 1 argument
        if args[0] == 'create':
            send_req('create', {'file_path': args[1]})
        elif args[0] == 'rm':
            send_req('rm', {'file_path': args[1]})
        elif args[0] == 'info':
            send_req('info', {'file_path': args[1]})
        elif args[0] == 'cd':
            send_req('cd', {'dir_path': args[1]})
        elif args[0] == 'ls':
            send_req('ls', {'dir_path': args[1]})
        elif args[0] == 'mkdir':
            send_req('mkdir', {'dir_path': args[1]})
        elif args[0] == 'rmdir':
            delete_directory(args[1])
        elif args[0] == 'write':
            write_file(args[1])
        else:
            print("Incorrect command!\nFor help write command: help")
    elif len(args) == 3:  # commands with 2 arguments
        if args[0] == 'login':
            send_req('login', {'username': args[1], 'password': args[2]})
        elif args[0] == 'read':
            read_file(args[1], args[2])
        elif args[0] == 'write':
            write_file(args[1], args[2])
        elif args[0] == 'copy':
            send_req('copy', {'path_from': args[1], 'path_to': args[2]})
        elif args[0] == 'mv':
            send_req('move', {'path_from': args[1], 'path_to': args[2]})
        else:
            print("Incorrect command!\nFor help write command: help")
    else:
        print("Wrong amount of arguments!\nFor help write command: help")


if __name__ == "__main__":
    main()
