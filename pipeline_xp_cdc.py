
###### CONFIG E IMPORTACAO ######
import dlt
import requests
import duckdb
from dotenv import load_dotenv
import pandas as pd

from datetime import datetime
import sys
import os
from src.ingestion_xp import get_data_aum, get_portfolios, autenticar, get_data_posicao
from src.utils import generate_date_dict
from pandas.tseries.offsets import BDay

load_dotenv()

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

def get_transformation(data, cols_to_pk = [], method="json"):

    if method == "json":
        df = pd.json_normalize(data)
    elif method == "list":
        df = pd.concat(data)
    #primary_key = 
    df["primary_key"] = df[cols_to_pk].astype(str).agg('-'.join, axis=1)
    return df

if __name__ == "__main__":
                
        ######################## utils #######################################
        ano = datetime.now().year
        mes =datetime.now().month        
        dates = generate_date_dict(ano, mes, ano, mes)
        periodo = datetime.date(datetime.now()) - BDay(5)
        periodo = datetime.strftime(periodo, format="%Y-%m-%d")

        ######################## PARAMS #######################################
        authorization = autenticar()
        portfolios_ids = get_portfolios(authorization)
        params = {
            "startReferenceDate": periodo,
            "endReferenceDate": periodo,
            "productTypes":["Fund","PensionFunds","FixedIncome","Repo",
                            "TradedFunds","Stock","Cash", "Treasury", "Coe"]
        }

        ######################## GET DATA #######################################
        data_aum = get_data_aum(portfolios_ids, dates, authorization)
        data_posicao = get_data_posicao(portfolios_ids, params, authorization)

        ######################## TRANSFORMATION ##################################
        cols_to_pk_aum = ["clientId", "effectiveDate"]
        cols_to_pk_posicao = ["clientId", "effectiveDate", "assetId"]
        df_aum = get_transformation(data_aum, cols_to_pk_aum, method="json")
        df_posicao = get_transformation(data_posicao, cols_to_pk_posicao, method="list")
        

        # ######################## LOAD ############################################
        conn = duckdb.connect("md:dwm_wealth")
        generate_table(conn, df_aum, "ap_xp_aum")
        generate_table(conn, df_posicao, "api_xp_posicao")

        conn.close()  
    