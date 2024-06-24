#!/bin/bash

# Check if at least one argument is provided
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 [mysql options] <database.table>"
  exit 1
fi

# Extract the table name (the last argument)
table_name="${@: -1}"

# Check if the table name contains a dot, indicating it is in the form of database.table
if [[ ! "$table_name" =~ \. ]]; then
  echo "Error: The last argument must be in the form of database.table"
  exit 1
fi

# Create a named pipe
pipe_name=$(mktemp -u)
mkfifo "$pipe_name"

# Set up gzip to read from the pipe and write to a file
gzip -c < "$pipe_name" > table_name.csv.gz &

# Extract all arguments except the last one (the table name)
mysql_args=("${@:1:$#-1}")

# Run the MySQL command to export the table to the named pipe
mysql "${mysql_args[@]}" -e "SELECT * FROM $table_name INTO OUTFILE '$pipe_name' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';"

# Remove the named pipe
rm "$pipe_name"
