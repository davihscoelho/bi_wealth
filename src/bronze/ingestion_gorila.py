import requests
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
import json
from pandas.tseries.offsets import BDay
from time import sleep
#from send_to_duckdb import create_schema, ingest_full_load_table, ingest_cdc_load_table, create_unique_index
import duckdb



load_dotenv()

def auth():

    base_url = "https://core.gorila.com.br"
    API_KEY = os.getenv("API_KEY_GORILA")
    authorization = {"Authorization": f"{API_KEY}"}

    return authorization

def get_data(url, authorization, params=None):

    response = requests.get(url, headers=authorization, params=params)
    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        return response.json()
    else:
        print(f"❌ Erro {response.status_code}: {response.text}")
        return None

#### GET PORTFOLIOS ####
def get_portfolios():

    authorization = auth()
    url = "https://core.gorila.com.br/portfolios"

    response = get_data(url, authorization)
    if response:
        print("Extracting portfolios IDs...")
        return response["records"]
    else:
        return None

authorization = auth()
portfolios_ids = get_portfolios()
conn = duckdb.connect("md:dwm_wealth")

########################## GET AUM ########################################
SCHEMA_NAME = "bronze"
TABLE_NAME = "aum"
PREFIX_NAME = "api_gorila"
PRIMARY_KEYS = ["referenceDate", "portfolio_id"]

PARAMS_AUM = {
    "startDate": "2024-01-01",
    "endDate": "2025-02-28",
    "frequency": "DAILY",
}

all_data = []
for portfolio_id in portfolios_ids[:3]:
#    print(portfolio_id)
    URL_AUM = F"https://core.gorila.com.br/portfolios/{portfolio_id["id"]}/nav"
    data = get_data(URL_AUM, authorization, params=PARAMS_AUM)

    if data:
        df = pd.DataFrame(data["timeseries"])
        df["portfolio_id"] = portfolio_id["id"]
        df["updated_at"] = datetime.now()
        
        print(f"Extracting data from AuM_API for client {portfolio_id['name']} at {datetime.now()}")
        all_data.append(df)

new_df = pd.concat(all_data)
create_schema(conn, SCHEMA_NAME)
ingest_full_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PREFIX_NAME)
create_unique_index(conn, SCHEMA_NAME, TABLE_NAME, f"{TABLE_NAME}_pk", PRIMARY_KEYS, PREFIX_NAME)
ingest_cdc_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PRIMARY_KEYS, PREFIX_NAME)




# ########################## GET POSITION ########################################
PARAMS_POSITION = {
                "referenceDate": "2025-01-28"
        }
all_data = []
for portfolio_id in portfolios_ids[:3]:
#    print(portfolio_id)
    URL_AUM = F"https://core.gorila.com.br/portfolios/{portfolio_id["id"]}/nav"
    data = get_data(URL_AUM, authorization, params=PARAMS_AUM)

    if data:
        
        df = pd.json_normalize(data["records"])
        df["portfolio_id"] = portfolio_id["id"]
        df["updated_at"] = datetime.now()
        
        print(f"Extracting data from AuM_API for client {portfolio_id['name']} at {datetime.now()}")
        all_data.append(df)

pd.concat(all_data)











# Testing
params_position_api = {
                "referenceDate": "2025-01-28"
        }
client_id = portfolios_ids[0]["id"]
URL_AUM = F"https://core.gorila.com.br/portfolios/{client_id}/positions/market-values"
data = get_data(URL_AUM, authorization, params=params_position_api)

data.keys()
teste = data["records"]
pd.teste
pd.json_normalize(teste)
teste["client_id"] = client_id
teste
pd.DataFrame(data["timeseries"]).info()