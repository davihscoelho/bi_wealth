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
        # Authentication
        credentials = Credentials.from_service_account_file(
            credentials_path, 
            scopes=scopes
        )
        print("✓ Credentials loaded")
        
        # Create client
        client = gspread.authorize(credentials)
        print("✓ Client authorized")
        
        try:
            # Open spreadsheet
            spreadsheets = client.open_by_key(spreadsheet_key)
            print(f"✓ Spreadsheet opened: {spreadsheets.title}")
            
            try:
                # Open worksheet
                if worksheet_name:
                    worksheet = spreadsheets.worksheet(worksheet_name)
                    print(f"✓ Worksheet opened: {worksheet.title}")
                else:
                    worksheet = spreadsheets.sheet1
                
                try:
                    # Get data
                    if range_name:
                        data = worksheet.get(range_name)
                        headers = data[0]
                        values = data[1:]
                        df = pd.DataFrame(values, columns=headers)
                        print(f"✓ Data fetched from range {range_name}")
                    else:
                        data = worksheet.get_all_records()
                        df = pd.DataFrame(data)
                    return df
                except Exception as e:
                    print("Error getting data from range:")
                    print(e)
                    return None
                    
            except Exception as e:
                print("Error opening worksheet:")
                print(e)
                return None
                
        except Exception as e:
            print("Error opening spreadsheet:")
            print(e)
            return None
            
    except Exception as e:
        print("Error with authentication:")
        print(e)
        return None

# Test the function
credentials_path = "../credentials/wealthdados-d086d5c72015.json"
spreadsheet_key = "1_Q86alpY3cGJDUbdJO--zudvHCDMBWTqQLuU7sE4Zik"

df = get_sheet_data(
    credentials_path=credentials_path,
    spreadsheet_key=spreadsheet_key,
    worksheet_name="c_ativos",
    range_name="A1:J"
)

if df is not None:
    print("\nData fetched successfully!")
    print("\nFirst few rows:")
    print(df.head())  # Changed from df_range to df
    print("\nColumns:", df.columns.tolist())