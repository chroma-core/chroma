#!/bin/bash

REQUIREMENTS_FILE="requirements.txt"
PYPROJECT_FILE="pyproject.toml"

# Checking if the files exist
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Error: $REQUIREMENTS_FILE does not exist."
    exit 1
fi

if [ ! -f "$PYPROJECT_FILE" ]; then
    echo "Error: $PYPROJECT_FILE does not exist."
    exit 1
fi

# Extracting dependencies from requirements.txt
REQUIREMENTS_DEPS=$(grep -vE '^\s*#|^\s*$' "$REQUIREMENTS_FILE")

# Extracting dependencies from pyproject.toml using Python
PYPROJECT_DEPS=$(python - <<END
import toml

def extract_dependencies(data):
    dependencies = []

    # Look for dependencies in various possible locations within pyproject.toml
    if 'project' in data and 'dependencies' in data['project']:
        dependencies.extend(data['project']['dependencies'])

    return dependencies

try:
    with open('$PYPROJECT_FILE', 'r') as f:
        data = toml.load(f)
        deps = extract_dependencies(data)
        print('\n'.join(deps))
except Exception as e:
    print(e)
END
)

# Checking if there are any differences
if [ "$REQUIREMENTS_DEPS" == "$PYPROJECT_DEPS" ]; then
    echo "Dependencies in $REQUIREMENTS_FILE and $PYPROJECT_FILE are in sync."
else
    echo "Dependencies in $REQUIREMENTS_FILE and $PYPROJECT_FILE are NOT in sync."
    echo "Dependencies in $REQUIREMENTS_FILE:"
    echo "$REQUIREMENTS_DEPS"
    echo "Dependencies in $PYPROJECT_FILE:"
    echo "$PYPROJECT_DEPS"
fi
