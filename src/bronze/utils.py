import pandas as pd
import re
from datetime import datetime
import requests
import os
from dotenv import load_dotenv

load_dotenv()


def extract_date(col_name):
    match = re.search(r"\((\d{1,2}/\d{1,2}/\d{4})", col_name)  # Extract date within ()
    if match:
        return pd.to_datetime(match.group(1), format="%m/%d/%Y")  # Convert to datetime
    else:
        return datetime.today()  # Assign today's date for the last column

def transformation_addepar(file_path):

    df = pd.read_excel(file_path, skiprows=2)
    # Drop unnecessary column
    df = df.drop(columns=["% of Portfolio"])

    # Rename the first column as 'code_id'
    df.rename(columns={df.columns[0]: "code_id"}, inplace=True)

    # Extract dates from column headers
    date_columns = df.columns[1:]  # Exclude 'code_id'
    
    # Apply function to extract dates
    date_mapping = {col: extract_date(col) for col in date_columns}

    # Rename columns to keep only extracted dates
    df.rename(columns=date_mapping, inplace=True)

    # Convert to long format (melt)
    df_long = df.melt(id_vars=["code_id"], var_name="date", value_name="value")
    
    # Drop last row (sum of totals)
    df_long = df_long.iloc[:-1]
    
    return df_long

def create_schema(conn, schema_name):
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
    print(f'Schema {schema_name} created successfully')

def ingest_full_load_table(conn, df, table_name, schema_name, prefix_name):
#    conn = duckdb.connect('data.db')
    conn.register('df', df)
    conn.execute(f'CREATE TABLE IF NOT EXISTS {schema_name}.{prefix_name}_{table_name} AS SELECT * FROM df')
    print(f'Table {table_name} created successfully')

def ingest_cdc_load_table(conn, df, table_name, schema_name, primary_keys, prefix_name):
    
    conn.register('df', df)
    
    primary_key_str = ", ".join(primary_keys)

    columns_without_pk = [col for col in df.columns if col not in primary_keys]

    set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns_without_pk])

    upsert_query = f"""
        INSERT INTO {schema_name}.{prefix_name}_{table_name}
        SELECT * FROM df
        ON CONFLICT ({primary_key_str}) DO UPDATE SET {set_clause}
    """
    conn.execute(upsert_query)
    #clean
    conn.unregister('df')
    print(f'Table {table_name} updated successfully')

def create_unique_index(conn, schema_name, table_name, index_name, columns, prefix_name):
    # Convert column list into a comma-separated string
    columns_str = ", ".join(columns)
    
    # Generate SQL statement for creating the unique index
    create_index_query = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
        ON {schema_name}.{prefix_name}_{table_name} ({columns_str})
    """
    conn.execute(create_index_query)
    print(f"Unique index {index_name} created on {schema_name}.{prefix_name}_{table_name} ({columns_str})")

def auth_gorila():

    base_url = "https://core.gorila.com.br"
    API_KEY = os.getenv("API_KEY_GORILA")
    authorization = {"Authorization": f"{API_KEY}"}

    return authorization

def get_data_gorila(url, authorization, params=None):

    response = requests.get(url, headers=authorization, params=params)
    if response.status_code == 200:
        print("Request bem-sucedido!")
        return response.json()
    else:
        print(f"Erro {response.status_code}: {response.text}")
        return None

def get_portfolios_gorila(params):

    authorization = auth_gorila()
    url = "https://core.gorila.com.br/portfolios"

    response = get_data_gorila(url, authorization, params=params)
    if response:
        print("Extracting portfolios IDs...")
        return response["records"]
    else:
        return None

def auth_xp():
    
    client_id = os.getenv("CLIENT_ID_XP")
    client_secret = os.getenv("CLIENT_SECRETS_XP")
    header = {"Content-Type":"application/x-www-form-urlencoded"}
    data = {
            'grant_type': "client_credentials",
            "scope": 'api://xpcorretora.onmicrosoft.com/api-ws-assets-query-external-prd/.default',
            "client_id": client_id,
            "client_secret": client_secret
        }
    url = 'https://login.microsoftonline.com/cf56e405-d2b0-4266-b210-aa04636b6161/oauth2/v2.0/token'
    response = requests.post(url, headers=header, data=data)
    if response.status_code == 200:
        print("Request bem-sucedido!")
        print(f"Token Gerado: Status Code {response.status_code}")
        authorization =  response.json()["token_type"] + " " + response.json()["access_token"]
        return authorization
    else:
        print("Request mal-sucedido!")

def get_data_xp(url, authorization, params=None):

    headers = {"Authorization": authorization}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        print("Request bem-sucedido!")
        return response.json()
    else:
        print(f"Nenhum dado encontrado!")
        return None