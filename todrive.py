import os
import sys
import time
import csv
import io
import mysql.connector
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# Scopes required for the script
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_google_drive():
    creds = None
    if os.path.exists('token.json'):
        from google.oauth2.credentials import Credentials
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def connect_to_db(database, user, password, host):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        unix_socket='/var/run/mysqld/mysqld.sock'
    )

def fetch_rows(cursor, table_name, batch_size):
    cursor.execute(f"SELECT * FROM {table_name}")
    cnt = 0

    while True:
        rows = cursor.fetchmany(batch_size)
        cnt += len(rows)
        print("Read: %d / %d"%(len(rows), cnt))

        if not rows:
            break
        yield rows

class StreamingBuffer(io.BytesIO):
    
    def __init__(self, generator, fieldnames):
        super().__init__()
        self.generator = generator
        self.writer = csv.writer(io.TextIOWrapper(self, encoding='utf-8', write_through=True))
        self.writer.writerow(fieldnames)
        self.feed_more_data()

    def seek(self, offset, whence=io.SEEK_SET):
        print(f"seek(offset={offset}, whence={whence})")
        return super().seek(offset, whence)
        
    def feed_more_data(self):
        try:
            current_pos = self.tell()
            rows = next(self.generator)
            self.writer.writerows(rows)
            self.seek(current_pos)
        except StopIteration:
            print("done reading source...")
    
    def read(self, size=-1):
        pos0 = self.tell()
        data = super().read(size)
        if len(data) < size:
            self.feed_more_data()
            data += super().read(size - len(data))
        print("READ!!! req: %d, chunk: %d, pos: +%d, total: %d"%(size, len(data), self.tell()-pos0, len(self.getvalue())))
        return data

def upload_row_batch_to_drive(service, table_name, cursor, fieldnames, batch_size, chunk_size=256*1024):
    row_generator = fetch_rows(cursor, table_name, batch_size)
    buffer = StreamingBuffer(row_generator, fieldnames)
    
    file_metadata = {'name': f"{table_name}.csv"}
    media = MediaIoBaseUpload(buffer, mimetype='text/csv', chunksize=chunk_size, resumable=True)
    
    request = service.files().create(body=file_metadata, media_body=media, fields='id')
    
    while True:
        status, response = request.next_chunk()
        if status:
            print(f'Upload progress: {status.progress() * 100:.2f}%')
        if response:
            print(response)
            print(f'Upload completed: {response.get("id")}')
            break

def main():
    if len(sys.argv) < 3:
        print("Usage: python script.py <database_name> <table_name> [username] [password] [host]")
        sys.exit(1)

    database_name = sys.argv[1]
    table_name = sys.argv[2]
    user = sys.argv[3] if len(sys.argv) > 3 else os.getlogin()
    password = sys.argv[4] if len(sys.argv) > 4 else ''
    host = sys.argv[5] if len(sys.argv) > 5 else 'localhost'

    service = authenticate_google_drive()

    db_connection = connect_to_db(database_name, user, password, host)
    cursor = db_connection.cursor()
    
    try:
        batch_size = 30000  # Number of rows to fetch per batch
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        fieldnames = [i[0] for i in cursor.description]
        cursor.fetchone() # Fetch to free up the cursor
        upload_row_batch_to_drive(service, table_name, cursor, fieldnames, batch_size)

    finally:
        cursor.close()
        db_connection.close()

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
