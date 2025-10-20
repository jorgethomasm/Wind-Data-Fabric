#!/bin/bash
# Linux/Mac bash script to create DuckDB database from data files
# Usage: ./create_duckdb.sh <folder_name>
# Example: ./create_duckdb.sh Kelmarsh

if [ -z "$1" ]; then
    echo "Error: Missing folder name argument"
    echo "Usage: ./create_duckdb.sh <folder_name>"
    echo "Example: ./create_duckdb.sh Kelmarsh"
    exit 1
fi

FOLDER_NAME=$1

echo "Creating DuckDB database for folder: $FOLDER_NAME"
echo ""

python scripts/create_duckdb.py --folder "$FOLDER_NAME"

if [ $? -eq 0 ]; then
    echo ""
    echo "Success! Database created."
else
    echo ""
    echo "Error: Database creation failed."
    exit $?
fi
