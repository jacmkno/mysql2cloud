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

# Extract all arguments except the last one (the table name)
mysql_args=("${@:1:$#-1}")

# Determine the secure_file_priv setting
secure_file_priv=$(mysql "${mysql_args[@]}" -N -e "SHOW VARIABLES LIKE 'secure_file_priv'" | awk '{ print $2 }')

# Create a named pipe in the appropriate directory
if [ -n "$secure_file_priv" ] && [ "$secure_file_priv" != "NULL" ]; then
  pipe_dir="$secure_file_priv"
else
  pipe_dir="/tmp"
fi

pipe_name=$(mktemp -u "$pipe_dir/mysql_pipe.XXXXXX")
mkfifo "$pipe_name"

# Set up gzip to read from the pipe and write to a file
gzip -c < "$pipe_name" > table_name.csv.gz &

# Run the MySQL command to export the table to the named pipe
mysql "${mysql_args[@]}" -e "SELECT * FROM $table_name INTO OUTFILE '$pipe_name' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';"

# Remove the named pipe
rm "$pipe_name"
