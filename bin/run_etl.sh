#!/bin/bash

VENV_PATH="/Users/agny.nureza/Documents/pacmaan/intro-data-engineering/final-project-intro-data-engineer/venv/bin/activate"

source "$VENV_PATH"

PYTHON_SCRIPT="/Users/agny.nureza/Documents/pacmaan/intro-data-engineering/final-project-intro-data-engineer/main.py"

python "$PYTHON_SCRIPT" >> /Users/agny.nureza/Documents/pacmaan/intro-data-engineering/final-project-intro-data-engineer/log/logfile.log 2>&1