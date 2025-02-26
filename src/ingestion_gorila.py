import requests
from dotenv import load_dotenv
import os
import json
import pandas as pd
from datetime import datetime
from time import sleep
import dlt
from pandas.tseries.offsets import BDay


load_dotenv()
#script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of the script
token = os.getenv("MOTHERDUCK_TOKEN")

def autenticar():

  base_url = "https://core.gorila.com.br"
  API_KEY = os.getenv("API_KEY_GORILA")
  authorization = {"Authorization": f"{API_KEY}"}

  return authorization

def get_portfolios():
    
    authorization = autenticar()
    url = "https://core.gorila.com.br/portfolios"

    response = requests.get(url, headers=authorization)
    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        print("Extracting portfolios IDs...")
    #	print(response.json())  # Print the response content
    else:
        print(f"❌ Erro {response.status_code}: {response.text}")
    data = response.json()
    data = response.json()["records"] # Retorna um Dicionario com os códigos IDs
    return data

def get_portfolio_aum(base_url, authorization, portfolio_id, params=None):
    response = requests.get(
            f"{base_url}/portfolios/{portfolio_id}/nav",
            params=params,
            headers=authorization,
        )
    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        resposta = response.json()
        if not resposta.get("timeseries"):
            print(f"❌ Nenhum dado encontrado para {portfolio_id}!")
            return   # Exit the function if "timeseries" is empty or None
        for i in resposta["timeseries"]:
            i["updated_at"] = datetime.now().isoformat()
            i["portfolio_id"] = portfolio_id
        return resposta["timeseries"]
    #	print(response.json())  # Print the response content
    else:
        print(f"❌ Erro {response.status_code}: {response.text}")
        return None

def get_portfolio_position(base_url, authorization, portfolio_id, params=None):
    response = requests.get(
        f"{base_url}/portfolios/{portfolio_id}/positions/market-values",
        params=params,
        headers=authorization,
    )

    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        resposta = response.json()
        if not resposta.get("records"):
            print(f"❌ Nenhum dado encontrado para {portfolio_id}!")
            return   # Exit the function if "timeseries" is empty or None
        for i in resposta["records"]:
            i["updated_at"] = datetime.now().isoformat()
            i["portfolio_id"] = portfolio_id
        return resposta["records"]
    #	print(response.json())  # Print the response content
    else:
        print(f"❌ Erro {response.status_code}: {response.text}")
        return None

def get_retorno_financeiro(base_url, authorization, portfolio_id, params=None):

    response = requests.get(
        f"{base_url}/portfolios/{portfolio_id}/pnl",
        params=params,
        headers=authorization,
    )

    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        resposta = response.json()
        
        # Check if "timeseries" exists and contains data
        if not resposta.get("timeseries"):
            print(f"❌ Nenhum dado encontrado para {portfolio_id}!")
            return   # Exit the function if "timeseries" is empty or None
        
        # Proceed with processing the data if it contains values
        for i in resposta["timeseries"]:
            i["updated_at"] = datetime.now().isoformat()
            i["portfolio_id"] = portfolio_id
        
        return resposta["timeseries"]
    else:
        print(f"❌ Erro {response.status_code}: {response.text}")
        return None

def get_data(params, funcao, portfolios_ids, *args, **kwargs ):
    """
    Generic data extraction function.

    Parameters:
    - params (dict): Query parameters for the API call.
    - funcao (function): Function used to fetch data (like get_portfolio_aum).
    - *args: Additional positional arguments for the fetch function.
    - **kwargs: Additional keyword arguments for the fetch function.

    Returns:
    - all_data (list): Combined data from all API calls.
    """
    authorization = autenticar()
    #portfolios_ids = get_portfolios(authorization)
    base_url = "https://core.gorila.com.br"
    all_data = []

    for portfolio_id in portfolios_ids:
        data = funcao(base_url, authorization, portfolio_id["id"], params=params, *args, **kwargs)
        if data:
            print(f"Extracting data from {funcao.__name__} for client {portfolio_id['id']} at {datetime.now()}")
            all_data.extend(data)
        else:
            pass
    return all_data


# # ############################# SIMPLE TESTING #############################
# authorization = autenticar()
# portfolios_ids = get_portfolios(authorization)
# base_url = "https://core.gorila.com.br"
# portfolio_id = portfolios_ids[0]["id"]
# params_aum_api = {
#     "startDate": "2024-01-01",
#     "endDate": "2025-01-28",
#     "frequency": "MONTHLY",
#     "seriesType": "PER_PERIOD"
#     }

# for portfolio_id in portfolios_ids[:5]:
#     data = get_retorno_financeiro(base_url, authorization, portfolio_id["id"], params=params_aum_api)
#     print(data)
#     sleep(1)
# resposta = get_retorno_financeiro(base_url, authorization, portfolio_id, params=params_aum_api)
# print(resposta)

# all_data = []
# for portfolio_id in portfolios_ids[:5]:
#     data = get_retorno_financeiro(base_url, authorization, portfolio_id["id"], params=params_aum_api)
#     if data:
#         #print(f"Extracting data from {funcao.__name__} for client {portfolio_id['id']} at {datetime.now()}")
#         all_data.extend(data)
# pd.DataFrame(all_data)
#     return all_data
# # print(resposta)




# ### 
# dia = datetime.date(datetime.now()) - BDay(1)
# dia = print(datetime.strftime(dia, format="%Y-%m-%d"))
# params = {
#     "referenceDate": dia,
# }
# portfolio_id = portfolios_ids[0]["id"]
# response = requests.get(
#     f"{base_url}/portfolios/{portfolio_id}/positions",
#     params=params,
#     headers=authorization,
# )

# filename = 'teste.json'
# with open(f"../data/{filename}", 'w') as f:
#     json.dump(response.json(), f)

# print(response.json()["records"])
#data = get_portfolio_positions("https://core.gorila.com.br", autenticar(), portfolios_ids[0]["id"], params=params)
#data = extract_data(portfolios_ids[:5], authorization, base_url, params)