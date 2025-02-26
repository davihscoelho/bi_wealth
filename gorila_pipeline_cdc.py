import dlt
import requests
from src.ingestion_gorila import *
import duckdb
from dotenv import load_dotenv
load_dotenv()
from datetime import time

# conn = duckdb.connect("md:dwm_wealth")
#conn.execute("CREATE TABLE IF NOT EXISTS dwm_wealth.bronze.api_gorila_retorno_financeiro AS SELECT * FROM df")
def generate_table(conn, data, table_name): 

    #conn = duckdb.connect("md:dwm_wealth")
    # If `data` is a query result (like `rel_aum_table`), store it as a temporary table
    # Create table if not exists and insert the data
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS dwm_wealth.bronze.{table_name} AS 
        SELECT * FROM data
    """)
    conn.execute("CREATE OR REPLACE TEMPORARY TABLE temp_data AS SELECT * FROM data")
    
    conn.sql(f"""
        DELETE FROM dwm_wealth.bronze.{table_name}
        WHERE primary_key IN (SELECT primary_key FROM temp_data)
    """
    )
    
    # Add the timestamp
    data_with_timestamp = conn.query("""
        SELECT 
            *, 
            CURRENT_TIMESTAMP AS inserted_date
        FROM temp_data
    """)
    conn.sql(f"""
        INSERT INTO dwm_wealth.bronze.{table_name}
        SELECT * FROM data_with_timestamp
    """
    )
#    print(f"Tabela {table_name} criada com sucesso em dwm_wealth.bronze.{table_name}!")

params_aum_api = {
        "startDate": "2025-02-01",
        "endDate": "2025-02-01",
        "frequency": "DAILY",
    }
periodo = datetime.date(datetime.now())
periodo2 = "2025-02-01"
#ano = datetime.date(datetime.now()).year
#periodo = datetime(ano, mes, 28).date()
periodo = datetime.strftime(periodo, format="%Y-%m-%d")

print(mes)
print(ano)
print(periodo)
params_ret_api = {
        "startDate": periodo,
        "endDate": periodo,
        "frequency": "MONTHLY",
        "seriesType": "PER_PERIOD"
    }

portfolios_ids = get_portfolios()[:10]
data_aum = get_data(params_aum_api, get_portfolio_aum, portfolios_ids)
data_retorno_financeiro = get_data(params_ret_api, get_retorno_financeiro, portfolios_ids)

conn = duckdb.connect("md:dwm_wealth")
df_aum = pd.json_normalize(data_aum)
df_aum["primary_key"] = df_aum["portfolio_id"] + df_aum["referenceDate"]


df_rets = pd.json_normalize(data_retorno_financeiro)
df_rets["primary_key"] = df_rets["portfolio_id"] + df_rets["referenceDate"]
df_rets
generate_table(conn, df_aum, "api_gorila_aum")
generate_table(conn, df_rets, "api_gorila_retorno_financeiro")

conn.close()    