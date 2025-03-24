import os.path
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

import glob
from datetime import datetime
from dotenv import load_dotenv
from utils import create_schema, ingest_full_load_table, ingest_cdc_load_table, create_unique_index, transformation_addepar
import duckdb
import pandas as pd
load_dotenv()

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.readonly"]  # Updated scope to allow file downloads
script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of the script
project_root = os.path.abspath(os.path.join(script_dir, '../..'))  # Project directory
cred_path = os.path.join(project_root, "config", "gdrive_api.json")
token_path = os.path.join(project_root, "config", "token.json")
downloads_dir_addepar = os.path.join(project_root, "data","downloads", "addepar")  # Create a downloads directory
downloads_dir_avenue = os.path.join(project_root, "data","downloads", "avenue")  # Create a downloads directory

# Create downloads directory if it doesn't exist
if not os.path.exists(downloads_dir_addepar):
    os.makedirs(downloads_dir_addepar)

if not os.path.exists(downloads_dir_avenue):
    os.makedirs(downloads_dir_avenue)


def auth_gdrive():
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    return creds

def list_files_in_folder(service, folder_id=None, file_type=None):
    """List files in a specific folder with optional file type filter"""
    query = ""
    
    # If folder is specified, add it to query
    if folder_id:
        query += f"'{folder_id}' in parents"
    
    # If file type is specified, add it to query
    if file_type:
        if query:
            query += " and "
        if file_type == "xlsx":
            # For Excel files, use the MIME type
            query += "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    
    try:
        results = service.files().list(
            q=query,
            pageSize=100,
            fields="nextPageToken, files(id, name, mimeType)"
        ).execute()
        
        items = results.get("files", [])
        return items
    
    except HttpError as error:
        print(f"An error occurred: {error}")
        return []
    
def download_file(service, file_id, file_name, downloads_dir):
    """Download a file from Google Drive by its ID"""
    try:
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}% complete for {file_name}")
        
        # Save the file to the downloads directory
        file_path = os.path.join(downloads_dir, file_name)
        with open(file_path, "wb") as f:
            f.write(file.getvalue())
        
        return file_path
    
    except HttpError as error:
        print(f"An error occurred while downloading {file_name}: {error}")
        return None

def get_data_from_gdrive(xlsx_files, downloads_dir):
    if not xlsx_files:
        print("No Excel files found in the specified folder.")
    print(f"Found {len(xlsx_files)} Excel files:")
    for i, item in enumerate(xlsx_files, 1):
        print(f"{i}. {item['name']} ({item['id']})")

    files_to_download = xlsx_files
    for file in files_to_download:
            file_path = download_file(service, file['id'], file['name'], downloads_dir)
            if file_path:
                print(f"Successfully downloaded: {file_path}")
        
    print("Download process completed.")

############# GET DATA FROM GDRIVE ############################
creds = auth_gdrive()
service = build("drive", "v3", credentials=creds)
folder_id = "1sVxtCTcrO098woAh2JPah3vKzHLlQ6lx"
folder_id_avenue = "1UytkZ__VeGQWtDauboB93EWtTHcB_XNp"
xlsx_files  = list_files_in_folder(service, folder_id, "xlsx")
xlsx_files_avenue = list_files_in_folder(service, folder_id_avenue, "xlsx")
get_data_from_gdrive(xlsx_files, downloads_dir_addepar)
get_data_from_gdrive(xlsx_files_avenue, downloads_dir_avenue)


############# TRANSFORM DATA FROM ADDEPAR ############################
lista_xp_us = [
                "QXR137258",
                "QXR142258",
                "QXR335290",
                "QXR308610",
                "QXR335597",
                "QXR341165",
                "QXR145475",
                "QXR341686"
            ]
# Transformando dataframe Addepar Antes de Enviar para Duckdb
file_list = glob.glob(os.path.join(downloads_dir_addepar, "*.xlsx"))
if file_list:
    for file_path in file_list:
        df = transformation_addepar(file_path)
        df["updated_at"] = datetime.now()
        #print(df.head())

df = df.loc[df["code_id"].isin(lista_xp_us)]

# Processing: Send to duckdb
conn = duckdb.connect("md:dwm_wealth")
SCHEMA_NAME = "bronze"
TABLE_NAME = "xpus"
PREFIX_NAME = "api_gdrive"
PRIMARY_KEYS = ["code_id", "date"]

create_schema(conn, SCHEMA_NAME)
ingest_full_load_table(conn, df, TABLE_NAME, SCHEMA_NAME, PREFIX_NAME)
create_unique_index(conn, SCHEMA_NAME, TABLE_NAME, f"{TABLE_NAME}_pk", PRIMARY_KEYS, PREFIX_NAME)
ingest_cdc_load_table(conn, df, TABLE_NAME, SCHEMA_NAME, PRIMARY_KEYS, PREFIX_NAME)

print("Carga realizada com sucesso!")
conn.close()

############# TRANSFORM DATA FROM AVENUE ############################
# Transformando dataframe Avenue Antes de Enviar para Duckdb
lista_dfs = []
file_list = glob.glob(os.path.join(downloads_dir_avenue, "*.xlsx"))
if file_list:
    for file_path in file_list:
        df = pd.read_excel(file_path)

        df["updated_at"] = datetime.now()

        df.columns = df.columns.str.strip()

        df.columns = df.columns.str.replace(" ", "_")

        df["CPF"] = df["CPF"].astype(str)
        
        lista_dfs.append(df)
    new_df = pd.concat(lista_dfs)
df.columns
# # Processing: Send to duckdb
conn = duckdb.connect("md:dwm_wealth")
SCHEMA_NAME = "bronze"
TABLE_NAME = "avenue"
PREFIX_NAME = "api_gdrive"
PRIMARY_KEYS = ["Date", "CPF", "Nome_do_Produto"]

create_schema(conn, SCHEMA_NAME)
ingest_full_load_table(conn, df, TABLE_NAME, SCHEMA_NAME, PREFIX_NAME)
create_unique_index(conn, SCHEMA_NAME, TABLE_NAME, f"{TABLE_NAME}_pk", PRIMARY_KEYS, PREFIX_NAME)
ingest_cdc_load_table(conn, df, TABLE_NAME, SCHEMA_NAME, PRIMARY_KEYS, PREFIX_NAME)

print("Carga realizada com sucesso!")
conn.close()