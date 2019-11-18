#!/bin/bash

PRIVATE_IP=$(ip route get 8.8.8.8 | sed -n '/src/{s/.*src *\([^ ]*\).*/\1/p;q}')
python3 -u namenode.py --ip $PRIVATE_IP --lock_duration 300 --update_time 200