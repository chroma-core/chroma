#!/bin/bash

set -e

pip install -r requirements.txt -r requirements_dev.txt --no-input --quiet

mypy .
