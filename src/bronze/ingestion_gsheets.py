import gspread
import pandas as pd
import duckdb
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv
load_dotenv()

def auth():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Diretório do script
    project_root = os.path.abspath(os.path.join(script_dir, '../..'))  # Diretório do projeto
    cred_path = os.path.join(project_root, "config", "credentials.json")
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
    client = gspread.authorize(creds)

    return client

def get_data(URL, sheet_name, client):

    worksheet = client.open_by_url(URL)
    sheet = worksheet.worksheet(sheet_name)
    headers = sheet.row_values(1)
    data = sheet.get_all_records(expected_headers=headers)
    if data:
        df = pd.DataFrame(data)
    return df


def create_schema(conn, schema_name):
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
    print(f'Schema {schema_name} created successfully')

def ingest_full_load_table(conn, df, table_name, schema_name):
#    conn = duckdb.connect('data.db')
    conn.register('df', df)
    conn.execute(f'CREATE TABLE IF NOT EXISTS {schema_name}.api_gsheets_{table_name} AS SELECT * FROM df')
    print(f'Table {table_name} created successfully')

def ingest_cdc_load_table(conn, df, table_name, schema_name, primary_keys):
    
    conn.register('df', df)
    
    primary_key_str = ", ".join(primary_keys)

    columns_without_pk = [col for col in df.columns if col not in primary_keys]

    set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns_without_pk])

    upsert_query = f"""
        INSERT INTO {schema_name}.api_gsheets_{table_name}
        SELECT * FROM df
        ON CONFLICT ({primary_key_str}) DO UPDATE SET {set_clause}
    """
    conn.execute(upsert_query)
    #clean
    conn.unregister('df')
    print(f'Table {table_name} updated successfully')

def create_unique_index(conn, schema_name, table_name, index_name, columns):
    # Convert column list into a comma-separated string
    columns_str = ", ".join(columns)
    
    # Generate SQL statement for creating the unique index
    create_index_query = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
        ON {schema_name}.api_gsheets_{table_name} ({columns_str})
    """
    conn.execute(create_index_query)
    print(f"Unique index {index_name} created on {schema_name}.{table_name} ({columns_str})")

def transform_data_captacao(df):

    df.columns = df.columns.str.strip()
    # Transformar Mes Coluna em Mes LInhas
    melted_df = pd.melt(
        df,
        id_vars=["Dados", "BANKER", "ANO"],
        var_name="MES",
        value_name="Total"
    )

    #Transformar padrao Mes Brasil em Mes Numerico
    month_mapping = {
        "JAN": "01", "FEV": "02", "MAR": "03", "ABR": "04", "MAI": "05", "JUN": "06",
        "JUL": "07", "AGO": "08", "SET": "09", "OUT": "10", "NOV": "11", "DEZ": "12"
    }

    #Transformar no padrão de DateTime
    melted_df["MES_NUM"] = melted_df["MES"].str.upper().map(month_mapping)
    melted_df["DATA"] = melted_df["ANO"].astype(str) + "-" + melted_df["MES_NUM"]
    melted_df["DATA"] = pd.to_datetime(melted_df["DATA"], format="%Y-%m")
    melted_df.drop(columns=["MES_NUM"], inplace=True)
    melted_df = melted_df.loc[melted_df["MES"] !=""]
    melted_df = melted_df.drop(columns=["ANO", "MES"])

    return melted_df

# Auth
client = auth()
#conn = duckdb.connect()
conn = duckdb.connect("md:dwm_wealth")

#PARAMETERS
URL_SHEET = "https://docs.google.com/spreadsheets/d/1_Q86alpY3cGJDUbdJO--zudvHCDMBWTqQLuU7sE4Zik/edit?gid=972946548#gid=972946548"
SHEET_NAME = ["c_ativos","faturamento","meta","captacao"]
SCHEMA_NAME = "bronze"
TABLE_NAME = ["clientes","faturamento","meta","captacao"]
PRIMARY_KEYS = [["CONTA"], ["DATA"], ["DATA"], ["DATA", "Dados", "BANKER"]]

for sheet, table, primary_key in zip(SHEET_NAME, TABLE_NAME, PRIMARY_KEYS):

    # GET and Tranform Data from Google Sheets to DataFrame
    if sheet == "captacao":
        df = get_data(URL_SHEET, sheet, client)
        df = df.drop(columns=[""], errors="ignore")
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(" ", "_")
        df = transform_data_captacao(df)


    else:
        df = get_data(URL_SHEET, sheet, client)
        df.columns = df.columns.str.replace(" ", "_")
        df = df.drop(columns=[""], errors="ignore")

    # Processing
    create_schema(conn, SCHEMA_NAME)
    ingest_full_load_table(conn, df, table, SCHEMA_NAME)
    create_unique_index(conn, SCHEMA_NAME, table, f"{table}_pk", primary_key)
    ingest_cdc_load_table(conn, df, table, SCHEMA_NAME, primary_key)
    
    # Testing
    conn.sql(f"SELECT * FROM bronze.api_gsheets_{table}")
print("Carga realizada com sucesso!")
conn.close()
# script_dir = os.path.dirname(os.path.abspath(__file__))  # Diretório do script
# project_root = os.path.abspath(os.path.join(script_dir, '../..'))  # Diretório do projeto
# cred_path = os.path.join(project_root, "config", "credentials.json")
# print(cred_path)  
#auth()