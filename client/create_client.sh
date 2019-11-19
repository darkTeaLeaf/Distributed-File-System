#!/bin/bash
sudo mkdir meowfs
cd meowfs
sudo wget https://raw.githubusercontent.com/darkTeaLeaf/Distributed-File-System/master/client/client.py
#sudo cp ~/dfs/client/client.py client.py
sudo wget https://raw.githubusercontent.com/darkTeaLeaf/Distributed-File-System/master/client/meowfs.spec

sudo pip3 install pyinstaller
sudo pip3 install requests
sudo pip3 install tcp_latency

sudo pyinstaller meowfs.spec
sudo rm -r __pycache__
sudo rm client.py
sudo rm -r build
sudo cp dist/meowfs /usr/bin/meowfs
sudo rm -r dist
sudo rm meowfs.spec
cd ../
sudo rm -r ./meowfs
