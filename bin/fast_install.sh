#!/bin/bash
# ================================================================================
# Environment Installation
# for use in TROC Navigator
#
# Copyright © 2020 Jesús Lara Giménez (phenobarbital) <jesuslarag@gmail.com>
# Version: 0.1
#
#    Developed by Jesus Lara (phenobarbital) <jesuslara@phenobarbital.info>
#
#    License: GNU GPL version 3  <http://gnu.org/licenses/gpl.html>.
#    This is free software: you are free to change and redistribute it.
#    There is NO WARRANTY, to the extent permitted by law.
# ================================================================================
#

sudo apt install -y  $(cat INSTALL)
#sudo apt install -y python3.8-dev python3.8-venv libmemcached-dev zlib1g-dev build-essential python-dev libffi-dev unixodbc unixodbc-dev
python3.8 -m venv .venv
source .venv/bin/activate -m
pip install --upgrade pip
# installing asyncdb
python setup.py develop
# install other requirements
pip install --use-feature=2020-resolver -r requirements.txt
