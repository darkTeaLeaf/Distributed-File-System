#!/bin/bash

PRIVATE_IP=$(ip route get 8.8.8.8 | sed -n '/src/{s/.*src *\([^ ]*\).*/\1/p;q}')
python3 ftp_server.py --ip $PRIVATE_IP --homedir /home/ubuntu/storage --namenode_ip 10.0.15.10