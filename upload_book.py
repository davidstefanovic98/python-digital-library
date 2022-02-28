import io
import os
import re
from itertools import islice

import pandas as pd
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, HttpRequest

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.appdata',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive.readonly',
          'https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.appdata',
          'https://www.googleapis.com/auth/drive.metadata',
          'https://www.googleapis.com/auth/drive.photos.readonly',
          'https://www.googleapis.com/auth/drive.scripts'
          ]
data = {

}

def are_columns_null(row):
    return pd.isna(row["Naslov"]) and \
           pd.isna(row["Godina"]) and \
           pd.isna(row["Opis"])


def are_not_float(row):
    return not isinstance(row["Opis"], float) and \
           not isinstance(row["Naslov"], float) and \
           not isinstance(row["Godina"], float) and \
           not isinstance(row["Link do knjige"], float)


def read_books():
    split_pattern = re.compile(r"\s*([,\n·]+|\s+y\s+|\s+and\s+|\s+&\s+)\s*")

    df = pd.read_csv("data.csv", header=1)
    iterator = islice(df.iterrows(), 0, None)

    for i, row in iterator:
        if not are_columns_null(row) and are_not_float(row):
            strindex = str(len(data.values()) + 1)
            data[strindex] = {
                "Id": strindex,
                "Link do knjige": None
            }
            if not pd.isna(row["Link do knjige"]):
                for link in split_pattern.split(str(row["Link do knjige"])):
                    link = link.strip(" \n\t\r")
                    data[strindex]["Link do knjige"] = link
                    file = download_file(link)
                    upload_book(strindex, file)
    # print(data)


def download_file(google_link: str):
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('drive', 'v3', credentials=creds)
        file_id = re.search("(d/)(<?.*)(/view)", google_link)[2]
        request: HttpRequest = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))
        return fh

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')


def create_pdf_file(bytes):
    file = open('file.pdf', 'wb')
    file.write(bytes.getvalue())
    file.close()
    return file


def upload_book(book_id, fileupload):
    print(fileupload)
    return requests.post("http://localhost:8080/isum/library-module/" + book_id + "/upload-file", files=dict(file=fileupload.getvalue()))


def main():
    read_books()


if __name__ == '__main__':
    main()
