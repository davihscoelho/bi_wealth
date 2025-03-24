# import pandas as pd
# import duckdb
# from dotenv import load_dotenv
# from datetime import datetime
# from utils import create_schema, ingest_full_load_table, create_unique_index, ingest_cdc_load_table, auth_gorila, get_data_gorila, get_portfolios_gorila


# conn = duckdb.connect("md:dwm_wealth")
# ########################## GET AUM ########################################
# authorization = auth_gorila()

# SCHEMA_NAME = "bronze"
# TABLE_NAME = "ativos"
# PREFIX_NAME = "api_gorila"
# PRIMARY_KEYS = ["id"]

# # PARAMS = {
# #     "limit": 2000
# # }


# URL = "https://core.gorila.com.br/securities"
# data = get_data_gorila(URL, authorization)

# pd.DataFrame(data["records"])
# len(data["records"])


# new_df = pd.DataFrame(data["records"])
# new_df = new_df.drop_duplicates(subset="id")

# create_schema(conn, SCHEMA_NAME)
# ingest_full_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PREFIX_NAME)
# create_unique_index(conn, SCHEMA_NAME, TABLE_NAME, f"{TABLE_NAME}_pk", PRIMARY_KEYS, PREFIX_NAME)
# ingest_cdc_load_table(conn, new_df, TABLE_NAME, SCHEMA_NAME, PRIMARY_KEYS, PREFIX_NAME)
# conn.close()


