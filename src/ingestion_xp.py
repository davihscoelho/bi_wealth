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
def get_portfolios(authorization):

    #authorization = autenticar()
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
        for i in resposta["profit"]:
            i["requested_at"] = datetime.now().isoformat()
            i["portfolio_id"] = portfolio_id
        return resposta["profit"]
    else:
        print(f"❌ Nenhum dado encontrado para {portfolio_id} no periodo {params}!")
        return 

def get_posicao_ativos(base_url, authorization, portfolio_id, params=None):
    headers = {"Authorization": authorization}
    response = requests.get(
            url = f"{base_url}/v2/positions/customers/{portfolio_id}",
            params=params,
            headers=headers
        )
    print(response)
    if response.status_code == 200:
        print("✅ Request bem-sucedido!")
        resposta = response.json()
        return resposta

    else:
        print(f"❌ Nenhum dado encontrado para {portfolio_id} no periodo {params}!")
        return 

def get_data_aum(portfolios_ids, dates, authorization):
    base_url = "https://openapi.xpi.com.br/ws-external-reports/api"
    #authorization = autenticar()    
    # from utils import generate_date_dict
    # dates = generate_date_dict(2024, 1, 2025, 1)
    lista_check = []
    for portfolio_id in portfolios_ids:       
        portfolio_id = portfolio_id["customerCode"] 
        for key, value in dates.items():
            print(f"Extracting data of {portfolio_id} from {key} at {datetime.now()} with range {value}")
            response = get_evolucao_aum(base_url, authorization, portfolio_id, params=value)
            if response:
                lista_check.extend(response)
            else:
                pass
            #print(key, value)
        pass
    return lista_check# pd.DataFrame(lista_check)

def get_data_posicao(portfolios_ids, params, authorization):

    base_url ="https://openapi.xpi.com.br/ws-assets-query/external/api"            
    lista_check = []
    
    for portfolio_id in portfolios_ids:
        portfolio_id = portfolio_id["customerCode"]
        print(f"Extracting data of {portfolio_id} at {datetime.now()}")
        response = get_posicao_ativos(base_url, authorization, portfolio_id, params)
        if response:
            dfs = []
            keys = list(response.keys())
            for key in response.keys():
                df = pd.json_normalize(response[key])
                if df.shape[0] > 0:    
                    print(f"Extracting data from {key} with shape {df.shape}")
                    dfs.append(df)
            if dfs:
                new_df = pd.concat(dfs)

                cols_to_drop = ["advisorCode", "originalDate", "originalValue", "openingQuantity", "openingUnitPrice", "openingValue", "originalQuantity",
                    "profitAndLoss", "yield", "previousYield", "accumulatedYield", "delta", "purchaseQuantity", "purchaseValue",
                    "saleQuantity", "saleValue", "status", "creationDate", "quotaVariation", "custodyTransferInQuantity", 
                    "custodyTransferInAmount", "adjusteQuotaQuantity",  "custodyTransferOutAmount","adjusteQuotaValue", 
                    "purchaseIncomeTax", "purchaseIof", "saleIncomeTax", "incomeTax", "iof", "saleIof", "sellingValue",
                    "purchasingValue", "strategy", "earningValue", "originalQuota", "custodyTransferOutQuantity", "marketPrice",
                    "invoiceId", "purchaseDate", "blockingQuantity", "collateralQuantity", "lawBlockingQuantity", "unitInterest", "totalInterest",
                    "unitAmortisation","remuneratedCustodyOpeningQuantity", "remuneratedCustodyOpeningValue","remuneratedCustodyClosingValue",
                    "remuneratedCustodyClosingQuantity", "totalAmortisation", "unitPremium", "totalPremium", "grossUpYield",
                    "originalUnitPrice", "isin", "iofRate", "incomeTaxRate", "grossUpAccumulatedYield", "grossUpCumulativeYield",
                    "cumulativeYield"]
                new_df = new_df.drop(columns=cols_to_drop, errors="ignore")
                if 'assetId' in new_df.columns:
                    new_df = new_df.dropna(axis=0, subset=['assetId'])
                lista_check.append(new_df)
    return lista_check

#lista_check.append()
    

########################### TESTING #######################
# authorization = autenticar()
# portfolios_ids = get_portfolios(authorization)
# portfolios_ids_teste = portfolios_ids[:25]
# portfolio_id = 208524  #5422 #["4583375","5422"]
# base_url ="https://openapi.xpi.com.br/ws-assets-query/external/api"
# params = {
#     "startReferenceDate": "2025-01-28",
#     "endReferenceDate": "2025-01-28",
#     "productTypes":["Fund","PensionFunds","FixedIncome","Repo","TradedFunds","Stock","Cash", "Treasury", "Coe"]
# }
# lista = get_data_posicao(portfolios_ids_teste, params, authorization)
# pd.concat(lista)
# pd.json_normalize(lista)
#portfolios_ids_teste
#get_posicao_ativos(base_url, authorization, portfolio_id, params)
