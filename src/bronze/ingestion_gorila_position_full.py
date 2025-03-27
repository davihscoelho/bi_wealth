import pandas as pd
import duckdb
from dotenv import load_dotenv
from datetime import datetime
from utils import create_schema, ingest_full_load_table, create_unique_index, ingest_cdc_load_table, auth_gorila, get_data_gorila, get_portfolios_gorila


conn = duckdb.connect("md:dwm_wealth")
########################## GET AUM ########################################
authorization = auth_gorila()
PARAMS = {
    "limit": 1000
}
portfolios_ids = get_portfolios_gorila(PARAMS)

SCHEMA_NAME = "bronze"
TABLE_NAME = "position"
PREFIX_NAME = "api_gorila"
PRIMARY_KEYS = ["referenceDate", "portfolio_id","security_name"]

# Get today's date
today = pd.Timestamp.today().normalize()

# Get the business day 3 days before today
last_business_day = (today - pd.offsets.BDay(3)).strftime("%Y-%m-%d")

PARAMNS = {
                "referenceDate": last_business_day
        }
all_data = []
for portfolio_id in portfolios_ids:
    URL = F"https://core.gorila.com.br/portfolios/{portfolio_id["id"]}/positions/market-values"
    data = get_data_gorila(URL, authorization, params=PARAMNS)

    if data:
        
        df = pd.json_normalize(data["records"])
        df["portfolio_id"] = portfolio_id["id"]
        df["updated_at"] = datetime.now().isoformat()
        df["primary_key"] = portfolio_id["id"]
        df["inserted_date"] = datetime.now().isoformat()
        
        print(f"Extracting data from Position for client {portfolio_id['name']} at {datetime.now()}")
        all_data.append(df)


new_df = pd.concat(all_data)
new_df.columns = new_df.columns.str.replace(".", "_")
new_df = new_df.drop_duplicates(subset=PRIMARY_KEYS)

create_schema(conn, SCHEMA_NAME)
ingest_full_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PREFIX_NAME)
create_unique_index(conn, SCHEMA_NAME, TABLE_NAME, f"{TABLE_NAME}_pk", PRIMARY_KEYS, PREFIX_NAME)
ingest_cdc_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PRIMARY_KEYS, PREFIX_NAME)
conn.close()
