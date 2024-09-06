#!/bin/bash

# Virtual Environment Path
VENV_PATH="/path/to/venv/bin/activate"

# Activate venv
source "$VENV_PATH"

# set python script
PYTHON_SCRIPT="/path/to/script/script.py"

# run python script and logging
python "$PYTHON_SCRIPT" >> /path/to/log/logfile.log 2>&1
