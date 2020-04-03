#!/bin/bash

virtualenv -p python3 venv
source venv/bin/activate
pip install --upgrade snowflake-connector-python
pip install pyyaml