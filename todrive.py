import csv
import os
import sys
import google.auth
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Scopes required for the script
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_google_drive():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = google.auth.load_credentials_from_file('token.json')
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', SCOPES)
            creds = flow.run_console()
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def create_csv(table_name):
    filename = f"{table_name}.csv"
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['column1', 'column2', 'column3']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(5):  # Example test data
            writer.writerow({'column1': f'data1_{i}', 'column2': f'data2_{i}', 'column3': f'data3_{i}'})
    return filename

def upload_file_to_drive(service, filename, folder_id=None):
    file_metadata = {'name': filename}
    if folder_id:
        file_metadata['parents'] = [folder_id]
    media = MediaFileUpload(filename, mimetype='text/csv')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f'File ID: {file.get("id")}')

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <table_name>")
        sys.exit(1)

    table_name = sys.argv[1]
    service = authenticate_google_drive()
    filename = create_csv(table_name)
    upload_file_to_drive(service, filename)
    os.remove(filename)  # Clean up local file

if __name__ == "__main__":
    main()
