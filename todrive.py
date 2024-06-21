import csv
import os
import sys
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

def authenticate_google_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    return GoogleDrive(gauth)

def create_csv(table_name):
    filename = f"{table_name}.csv"
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['column1', 'column2', 'column3']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(5):  # Example test data
            writer.writerow({'column1': f'data1_{i}', 'column2': f'data2_{i}', 'column3': f'data3_{i}'})
    return filename

def upload_file_to_drive(drive, filename, folder_id=None):
    file = drive.CreateFile({'title': filename, 'parents': [{'id': folder_id}] if folder_id else []})
    file.SetContentFile(filename)
    file.Upload()
    print(f'File {filename} uploaded successfully.')

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <table_name>")
        sys.exit(1)

    table_name = sys.argv[1]
    drive = authenticate_google_drive()
    filename = create_csv(table_name)
    upload_file_to_drive(drive, filename)
    os.remove(filename)  # Clean up local file

if __name__ == "__main__":
    main()
