import os.path
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]  # Updated scope to allow file downloads
script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of the script
project_root = os.path.abspath(os.path.join(script_dir, '../..'))  # Project directory
cred_path = os.path.join(project_root, "config", "gdrive_api.json")
token_path = os.path.join(project_root, "config", "token.json")
downloads_dir = os.path.join(project_root, "data","downloads")  # Create a downloads directory

# Create downloads directory if it doesn't exist
if not os.path.exists(downloads_dir):
    os.makedirs(downloads_dir)


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
    
def download_file(service, file_id, file_name):
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


creds = auth_gdrive()
service = build("drive", "v3", credentials=creds)
folder_id = "1sVxtCTcrO098woAh2JPah3vKzHLlQ6lx"
xlsx_files  = list_files_in_folder(service, folder_id, "xlsx")

if not xlsx_files:
    print("No Excel files found in the specified folder.")
print(f"Found {len(xlsx_files)} Excel files:")
for i, item in enumerate(xlsx_files, 1):
    print(f"{i}. {item['name']} ({item['id']})")

files_to_download = xlsx_files
for file in files_to_download:
        file_path = download_file(service, file['id'], file['name'])
        if file_path:
            print(f"Successfully downloaded: {file_path}")
    
print("Download process completed.")