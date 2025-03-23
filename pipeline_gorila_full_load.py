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
def get_transformation(data):
    df = pd.json_normalize(data)
    df["primary_key"] = df["portfolio_id"] + df["referenceDate"]
    return df

if __name__ == "__main__":
                
        ######################## utils #######################################
        periodo = datetime.date(datetime.now()) - BDay(3)
        periodo = datetime.strftime(periodo, format="%Y-%m-%d")
        portfolios_ids = get_portfolios()
        
        ######################## PARAMS #######################################
        params_aum_api = {
                "startDate": "2024-01-01",
                "endDate": "2025-01-28",
                "frequency": "DAILY",
            }

        params_ret_api = {
                "startDate": "2024-01-01",
                "endDate": "2025-01-28",
                "frequency": "MONTHLY",
                "seriesType": "PER_PERIOD"
            }

        params_position_api = {
                    "referenceDate": "2025-01-28"
            }

        ######################## GET DATA #######################################
        data_aum = get_data(params_aum_api, get_portfolio_aum, portfolios_ids)
        data_retorno_financeiro = get_data(params_ret_api, get_retorno_financeiro, portfolios_ids)
        data_position = get_data(params_position_api, get_portfolio_position, portfolios_ids)

        ######################## TRANSFORMATION ##################################
        df_aum = get_transformation(data_aum)
        df_rets = get_transformation(data_retorno_financeiro)
        df_posicao = get_transformation(data_position)

        ######################## LOAD ############################################
        conn = duckdb.connect("md:dwm_wealth")
        generate_table(conn, df_aum, "api_gorila_aum")
        generate_table(conn, df_rets, "api_gorila_retorno_financeiro")
        generate_table(conn, df_posicao, "api_gorila_position")

        conn.close()  
    
