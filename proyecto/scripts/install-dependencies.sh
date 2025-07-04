#!/usr/bin/bash 
set -e
cp requirements.txt /tmp/
cd /tmp/
sudo apt update
#install python dependencies
python3 -m pip install --upgrade pip setuptools wheel
pip3 install --user -r requirements.txt
