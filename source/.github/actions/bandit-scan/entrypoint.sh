#!/bin/bash
CFG="-c $2"
if [ -z "$1" ]; then
    echo "No path to scan provided"
    exit 1
fi

if [ -z "$2" ]; then
    CFG = ""
fi

bandit -f "$1" ${CFG} -r "$3" -o "$4"
exit 0 #we want to ignore the exit code of bandit (for now)
