import os
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from pymysqlreplication.event import QueryEvent
from google.cloud import bigquery
import pymysql

# Get the current active OS username
current_user = os.getenv('USER')

# MySQL connection settings using Unix socket
mysql_settings = {
    "unix_socket": "/var/run/mysqld/mysqld.sock",
    "user": current_user,
}

# BigQuery settings
bq_client = bigquery.Client()
dataset_id = 'your_dataset'
table_id = 'your_table'

def insert_into_bigquery(rows, event_type):
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)

    errors = bq_client.insert_rows_json(table, rows)
    if errors:
        print(f'Errors: {errors}')

def process_binlog_event(event):
    if isinstance(event, WriteRowsEvent):
        rows = [row["values"] for row in event.rows]
        insert_into_bigquery(rows, "INSERT")
    elif isinstance(event, UpdateRowsEvent):
        rows = [row["after_values"] for row in event.rows]
        insert_into_bigquery(rows, "UPDATE")
    elif isinstance(event, DeleteRowsEvent):
        rows = [row["values"] for row in event.rows]
        insert_into_bigquery(rows, "DELETE")

def get_primary_keys(database, table):
    connection = pymysql.connect(
        unix_socket='/var/run/mysqld/mysqld.sock',
        user=current_user
    )
    cursor = connection.cursor()
    cursor.execute(f"SHOW KEYS FROM `{database}`.`{table}` WHERE Key_name = 'PRIMARY'")
    primary_keys = [row[4] for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    return primary_keys

def handle_event(event_type, rows, primary_keys):
    for row in rows:
        if event_type == "INSERT":
            print(f"Insert event: {row}")
        elif event_type == "UPDATE":
            print(f"Update event: {row}, primary keys: {primary_keys}")
        elif event_type == "DELETE":
            print(f"Delete event: {row}")

def handle_query_event(query):
    query = query.lower()
    if query.startswith('create table'):
        table_name = query.split()[2]
        primary_keys = get_primary_keys('your_database', table_name)
        print(f"Table created: {table_name}, primary keys: {primary_keys}")
    elif query.startswith('drop table'):
        table_name = query.split()[2]
        print(f"Table dropped: {table_name}")
    elif query.startswith('alter table'):
        table_name = query.split()[2]
        primary_keys = get_primary_keys('your_database', table_name)
        print(f"Table altered: {table_name}, primary keys: {primary_keys}")

# Start reading the binlog
stream = BinLogStreamReader(
    connection_settings=mysql_settings,
    server_id=1,
    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
    log_file='mysql-bin.000001',
    log_pos=4,
    resume_stream=True
)

for binlogevent in stream:
    if isinstance(binlogevent, QueryEvent):
        handle_query_event(binlogevent.query)
    elif isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
        table_name = binlogevent.table
        primary_keys = get_primary_keys('your_database', table_name)
        rows = [row['values'] for row in binlogevent.rows]
        event_type = "INSERT" if isinstance(binlogevent, WriteRowsEvent) else "UPDATE" if isinstance(binlogevent, UpdateRowsEvent) else "DELETE"
        handle_event(event_type, rows, primary_keys)

stream.close()
