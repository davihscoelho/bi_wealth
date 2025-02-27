import dlt
import requests
from src.ingestion_gorila import *
import duckdb
from dotenv import load_dotenv
load_dotenv()
from datetime import time


def generate_table(conn, data, table_name): 

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
    print(f"Tabela {table_name} adicionada com sucesso em dwm_wealth.bronze.{table_name}!")

def get_transformation(data):
    df = pd.json_normalize(data)
    df["primary_key"] = df["portfolio_id"].astype(str) + df["referenceDate"].astype(str)
    return df

if __name__ == "__main__":
                
        ######################## utils #######################################
        periodo = datetime.date(datetime.now()) - BDay(3)
        periodo = datetime.strftime(periodo, format="%Y-%m-%d")
        portfolios_ids = get_portfolios()[:2]
        
        ######################## PARAMS #######################################
        params_aum_api = {
                "startDate": periodo,
                "endDate": periodo,
                "frequency": "DAILY",
            }

        params_ret_api = {
                "startDate": periodo,
                "endDate": periodo,
                "frequency": "MONTHLY",
                "seriesType": "PER_PERIOD"
            }

        params_position_api = {
                    "referenceDate": periodo
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