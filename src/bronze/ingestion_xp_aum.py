import pandas as pd
import duckdb
from datetime import time
from dotenv import load_dotenv
import os
import requests

from utils import auth_xp, get_data_xp, create_schema, ingest_full_load_table, ingest_cdc_load_table, create_unique_index


def get_portfolios():
    auth = auth_xp()

    URL = "https://openapi.xpi.com.br/wealthservices-contracts/external/api/v1/customers"
    header = {
        "Authorization": auth,
        "Ocp-Apim-Subscription-Key": "ada131f3cf3a41a2a0d8ce0048b43ad9"
    }
    response = requests.get(URL, headers=header)
    if response.status_code == 200:
        print("Request bem-sucedido!")
        print(f"Portfolios: Status Code {response.status_code}")
        return response.json()
    
portfolios = get_portfolios()


##################### TESTING #####################
auth = auth_xp()
portfolio_id = portfolios[0]["customerCode"]
URL = f"https://openapi.xpi.com.br/ws-external-reports/api/v1/wealth-evolution/customer/{portfolio_id}"

params = {
    "startDate": "2024-02-01",
    "endDate": "2024-02-28"
}
data = get_data_xp(URL, auth_xp(), params) 
df = pd.DataFrame(data["profit"])
df["portfolio_id"] = str(portfolio_id)
df["inserted_date"] = pd.Timestamp.now()


conn = duckdb.connect("md:dwm_wealth")

# Code for Full Loading
params_looping = []
for i in range(1,13,1):
    if i < 10:
        mes = f"0{i}"
        params_looping.append({
            "startDate": f"2024-{mes}-01",
            "endDate": f"2024-{mes}-28"
        })
    else:
        mes=f"{i}"
        params_looping.append({
            "startDate": f"2024-{mes}-01",
            "endDate": f"2024-{mes}-28"
        })

SCHEMA_NAME = "bronze"
TABLE_NAME = "aum"
PREFIX_NAME = "api_xp"
PRIMARY_KEYS = ["date", "portfolio_id"]

lista_df = []
for params in params_looping:
    data = get_data_xp(URL, auth_xp(), params) 
    df = pd.DataFrame(data["profit"])
    df["portfolio_id"] = str(portfolio_id)
    df["inserted_date"] = pd.Timestamp.now()
    lista_df.append(df)

new_df = pd.concat(lista_df)
conn.close()


create_schema(conn, SCHEMA_NAME)
ingest_full_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PREFIX_NAME)
create_unique_index(conn, SCHEMA_NAME, TABLE_NAME, f"{TABLE_NAME}_pk", PRIMARY_KEYS, PREFIX_NAME)
ingest_cdc_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PRIMARY_KEYS, PREFIX_NAME)

#new_df

# params_looping
# portfolio_id
# lista_df = []
# data = get_data_xp(URL, auth_xp(), params) 
# df = pd.DataFrame(data["profit"])
# df["portfolio_id"] = str(portfolio_id)
# df["inserted_date"] = pd.Timestamp.now()
# lista_df.append(df)
# df.info()
# print(portfolios)
