#!/bin/bash

# Function to create a virtual environment and install required packages
create_env() {
    python3 -m venv venv
    source venv/bin/activate
    pip3 install --upgrade pip
    pip3 install pydrive
    deactivate
}

# Check if the virtual environment already exists
if [ -d "venv" ]; then
    echo "Using existing virtual environment."
else
    echo "Creating a new virtual environment."
    create_env
fi

# Activate the virtual environment
source venv/bin/activate

# Indicate that the environment is ready
echo "Virtual environment activated. You can now run your script with: python3 script.py <table_name>"
