import requests
import os
import json
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay

from dotenv import load_dotenv
load_dotenv()

# Step by step
# 1. Autenticar
# 2. Get portfolios
# 3. Get portfolio AUM
# 4. Get data from endpoints
# 5. Save data to Datalake

def autenticar():
    
    client_id = os.getenv("CLIENT_ID_XP")
    client_secret = os.getenv("CLIENT_SECRETS_XP")
    header = {"Content-Type":"application/x-www-form-urlencoded"}
    data = {
            'grant_type': "client_credentials",
            "scope": 'api://xpcorretora.onmicrosoft.com/api-ws-assets-query-external-prd/.default',
            "client_id": client_id,
            "client_secret": client_secret
        }
    url = 'https://login.microsoftonline.com/cf56e405-d2b0-4266-b210-aa04636b6161/oauth2/v2.0/token'
    response = requests.post(url, headers=header, data=data)
    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        print(f"Token Gerado: Status Code {response.status_code}")
        authorization =  response.json()["token_type"] + " " + response.json()["access_token"]
        return authorization
    else:
        print("❌ Request mal-sucedido!")
def get_portfolios():

    authorization = autenticar()
    header = {
        "Authorization": authorization,
        "Ocp-Apim-Subscription-Key": 'ada131f3cf3a41a2a0d8ce0048b43ad9'

    }
    url_base = "https://openapi.xpi.com.br/wealthservices-contracts/external"
    url = f"{url_base}/api/v1/customers"
    response = requests.get(url, headers=header)
    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        print(f"Portfolios: Status Code {response.status_code}")
        return response.json()

def get_evolucao_aum(base_url, authorization, portfolio_id, params=None):
    headers = {"Authorization": authorization}
    response = requests.get(
            url = f"{base_url}/v1/wealth-evolution/customer/{portfolio_id}",
            params=params,
            headers=headers
        )
    
    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        resposta = response.json()
        # Proceed with processing the data if it contains values
        for i in resposta["timeseries"]:
            i["portfolio_id"] = portfolio_id
        return resposta
    else:
        print(f"❌ Nenhum dado encontrado para {portfolio_id} no periodo {params}!")
        return 


######################### TESTING #######################

authorization = autenticar()    
portfolios_ids = get_portfolios()
portfolios_ids
portfolio_id = portfolios_ids[-6]["customerCode"]
#portfolio_id = "3134083"
base_url = "https://openapi.xpi.com.br/ws-external-reports/api"
params = {
    "startDate": "2025-01-01",
    "endDate": "2025-01-28"
}
response = get_evolucao_aum(base_url, authorization, portfolio_id, params=params)

from utils import generate_date_dict
dates = generate_date_dict(2024, 1, 2025, 1)
dates





response
#response.keys()
response.json()
data = response["profit"]
data

data = pd.DataFrame(data)
data


#portfolio_id
#response = autenticar()
#response