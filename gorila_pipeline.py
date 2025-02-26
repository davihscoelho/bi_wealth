import dlt
import requests
from src.ingestion_gorila import *
import duckdb
from dotenv import load_dotenv
load_dotenv()
from datetime import time

def pipeline_gorila(pipeline_name, data, table_name, write_disposition="merge", primary_key=None):

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination='motherduck',
        dataset_name='bronze'
    )
    load_info = pipeline.run(
        data,
        table_name=table_name,
        primary_key=("portfolio_id", "referenceDate"),
        write_disposition=write_disposition,
    )
    print("Raw load_info:", load_info)
    # print(f"Pipeline '{pipeline_name}' executed.")
    # print(f"Status: {load_info['status']}")
    # print(f"Rows processed: {load_info.get('rows_processed', 'N/A')}")
    # print(f"Error: {load_info.get('error', 'None')}")

def create_schemas(conn):

    conn.execute(
        """
            CREATE SCHEMA IF NOT EXISTS bronze;
            CREATE SCHEMA IF NOT EXISTS silver;
            CREATE SCHEMA IF NOT EXISTS gold;
        """
    )
    return print("Esquemas criados com sucesso!")

def generate_table(conn, data, table_name): 

    #conn = duckdb.connect("md:dwm_wealth")
    # If `data` is a query result (like `rel_aum_table`), store it as a temporary table
    conn.execute("CREATE OR REPLACE TEMPORARY TABLE temp_data AS SELECT * FROM data")
    
    # Add the timestamp
    data_with_timestamp = conn.query("""
        SELECT 
            *, 
            CURRENT_TIMESTAMP AS inserted_date
        FROM temp_data
    """)

    # Create table if not exists and insert the data
    conn.execute(f"""
        CREATE OR REPLACE TABLE dwm_wealth.bronze.{table_name} AS 
        SELECT * FROM data_with_timestamp
    """)
    print(f"Tabela {table_name} criada com sucesso em dwm_wealth.bronze.{table_name}!")

    #conn.close()    

if __name__ == "__main__":
    
    params_aum_api = {
        "startDate": "2024-01-01",
        "endDate": "2025-01-28",
        "frequency": "DAILY",
    }
    dia = datetime.date(datetime.now()) - BDay(3)
    dia = datetime.strftime(dia, format="%Y-%m-%d")
    #dia = "2025-01-28"
    #print(dia)
    params_position_api = {
    "referenceDate": dia,
    }
    params_ret_api = {
        "startDate": "2024-01-01",
        "endDate": "2025-01-28",
        "frequency": "MONTHLY",
        "seriesType": "PER_PERIOD"
    }
    portfolios_ids = get_portfolios()
    data_aum = get_data(params_aum_api, get_portfolio_aum, portfolios_ids)
    data_position = get_data(params_position_api, get_portfolio_position, portfolios_ids)
    data_retorno_financeiro = get_data(params_ret_api, get_retorno_financeiro, portfolios_ids)
    
    conn = duckdb.connect("md:dwm_wealth")
    
    df_aum = pd.json_normalize(data_aum)
    df_aum["primary_key"] = df_aum["portfolio_id"] + df_aum["referenceDate"]
    df_posicao = pd.json_normalize(data_position)
    df_posicao["primary_key"] = df_posicao["portfolio_id"] + df_posicao["referenceDate"]
    df_rets = pd.json_normalize(data_retorno_financeiro)
    df_rets["primary_key"] = df_rets["portfolio_id"] + df_rets["referenceDate"]
    
    create_schemas(conn)
    generate_table(conn, df_aum, "api_gorila_aum")
    generate_table(conn, df_posicao, "api_gorila_position")
    generate_table(conn, df_rets, "api_gorila_retorno_financeiro")
    
    conn.close()
    
    
    
    
    # pipeline_gorila("gorila_aum", data_aum, "api_gorila_aum", "merge")
    # pipeline_gorila("gorila_position", data_position, "api_gorila_position", "merge")
    
    # data_retorno_financeiro = get_data(params_ret_api, get_retorno_financeiro)
    # df = pd.DataFrame(data_retorno_financeiro)
    # df["primary_key"] = df["portfolio_id"] + df["referenceDate"]
    # conn = duckdb.connect("md:dwm_wealth")
    # conn.execute("CREATE TABLE IF NOT EXISTS dwm_wealth.bronze.api_gorila_retorno_financeiro AS SELECT * FROM df")
    
    # incremental_table = df.copy()
    # #conn.sql("SELECT * FROM dwm_wealth.bronze.api_gorila_retorno_financeiro").show()
    # conn.execute("CREATE OR REPLACE TEMP TABLE incremental_table AS SELECT * FROM df")
    # conn.sql("""
    #     DELETE FROM dwm_wealth.bronze.api_gorila_retorno_financeiro
    #     WHERE primary_key in (SELECT primary_key FROM incremental_table)
    # """
    # )
    # conn.sql(
    # """
    #     INSERT INTO dwm_wealth.bronze.api_gorila_retorno_financeiro
    #     SELECT * FROM incremental_table
    # """
    # )
    #conn.sql("SELECT * FROM dwm_wealth.bronze.api_gorila_retorno_financeiro").show()
    #conn.close()
    
    #pipeline_gorila("gorila_retorno_financeiro", data_retorno_financeiro, "api_gorila_retorno_financeiro", "merge")

    #pipeline_test()
    #pipeline_gorila()