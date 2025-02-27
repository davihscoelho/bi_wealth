
###### CONFIG E IMPORTACAO ######
import dlt
import requests
import duckdb
from dotenv import load_dotenv
import pandas as pd

from datetime import datetime
import sys
import os
from src.ingestion_xp import get_data_aum, get_portfolios, autenticar
from src.utils import generate_date_dict

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
def get_transformation(data):
    df = pd.json_normalize(data)
    df["primary_key"] = df["portfolio_id"].astype(str) + df["date"].astype(str)
    return df

if __name__ == "__main__":
                
        ######################## utils #######################################
        ano = datetime.now().year
        mes =datetime.now().month        
        ######################## PARAMS #######################################
        authorization = autenticar()
        
        dates = generate_date_dict(ano, mes, ano, mes)
        portfolios_ids = get_portfolios(authorization)
        portfolios_ids_teste = portfolios_ids

        ######################## GET DATA #######################################
        data_aum = get_data_aum(portfolios_ids_teste, dates, authorization)


        ######################## TRANSFORMATION ##################################
        df_aum = get_transformation(data_aum)
        
        

        # ######################## LOAD ############################################
        conn = duckdb.connect("md:dwm_wealth")
        generate_table(conn, df_aum, "ap_xp_aum")
        
        conn.close()  
    