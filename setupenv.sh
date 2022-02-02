#!/usr/bin/env bash
python3 -m venv ./env
source env/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements-dev.txt
pip3 install -r mqclient/requirements.txt
