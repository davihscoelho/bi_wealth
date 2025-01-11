import pandas as pd
import duckdb as db
import gspread
from google.oauth2.service_account import Credentials

def get_sheet_data(
	credentials_path: str,
	spreadsheet_key: str,
	worksheet_name: str = None,
	range_name: str = None	

):

	scopes = [
		"https://www.googleapis.com/auth/spreadsheets",
		"https://www.googleapis.com/auth/drive"
	]

	try:
		credentials = Credentials.from_service_account_file(
			credentials_path, 
			scopes=scopes
		)
		print(credentials)
		# Criar gspread client
		client = gspread.authorize(credentials)
		print(client)
		# Abrir PLanilha
		spreadsheets = client.open_by_key(spreadsheet_key)
		print(spreadsheets)
		# Abrir Aba
		if worksheet_name:
			worksheet = spreadsheets.worksheet(worksheet_name)
			print(worksheet)
		else:
			worksheet = spreadsheets.sheet1 # Pega a primeira planilha 

		# Setar Range 
		if range_name:
			data = worksheet.get(range_name)
			headers = data[0]
			values = data[1:]
			df = pd.DataFrame(values, columns=headers)
			print(df)
		else:
			data = worksheet.get_all_records()
			df = pd.DataFrame(data)
		return df
	except Exception as e:
		print("Erro ao buscar dados da planilha")
		print(e)
		return None

		# credentials = Credentials.from_service_account_file("credentials.json", scopes=scopes)
		# gc = gspread.authorize(credentials)
		# sh = gc.open_by_key("1c8v1mZb9Zn4Jj1X4jV4jBh8J6D8YJ1X4jV4jBh8J6D8")
		# worksheet = sh.get_worksheet(0)
		# data = worksheet.get_all_records()
		# return pd.DataFrame(data)

def save_data_locally(df, folder_name="../data/gsheets", file_name="sheet_data.csv"):
    try:
        # Ensure the folder exists
        os.makedirs(folder_name, exist_ok=True)
        # Define the file path
        file_path = os.path.join(folder_name, file_name)
        # Save the DataFrame as a CSV file
        df.to_csv(file_path, index=False)
        print(f"Data saved successfully to {file_path}")
    except Exception as e:
        print("Erro ao salvar dados localmente")
        print(e)



credentials_path = "../credentials/wealthdados-d086d5c72015.json"
spreadsheet_key = "1_Q86alpY3cGJDUbdJO--zudvHCDMBWTqQLuU7sE4Zik"
file_name = "c_ativos.csv"

df = get_sheet_data(
		credentials_path=credentials_path,
		spreadsheet_key=spreadsheet_key,
		worksheet_name="c_ativos",
		range_name="A1:J"
		)
if df is not None:
	print("Data fetched successfully!")
	print("\nFirst few rows:")
	print(df.head())
	print("\nColumns:", df.columns.tolist())
	save_data_locally(df, file_name=file_name)