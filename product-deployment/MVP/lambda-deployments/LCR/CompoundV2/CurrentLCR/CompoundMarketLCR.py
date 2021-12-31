import os
import json
import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
## Formatted
def main():

    transactionResponse = {'status':200,'response':{
                           'data':{'COMPOUND':{"TOTAL_LIABILITIES":[],"TOTAL_ASSETS":[],"MARKET_CAP":[]}},
                           'result':{'COMPOUND':{'data':{'v2_ratio':None,}}}}}


    names ={"DAI"  : 'dai',
            "SAI"   : 'sai',
            "REP"   : 'augur',
            "TUSD"  : 'true-usd',
            "USDC"  : 'usd-coin',
            "USDT"  : "tether",
            "ETH"   : "ethereum",
            "LINK"  : "chainlink",
            "MKR"   : "maker",
            "RAI"   : "rai",
            "UNI"   : "uniswap",
            "WBTC"  : "wrapped-bitcoin",
            "SUSHI": "sushi",
            "YFI"   : "yearn-finance",
            "AAVE"  : "aave",
            "BAT"   : "basic-attention-token",
            "ZRX"   : "0x",
            # "COMP"  : "compound"
        }

    ind  = 0 
    res  = requests.get("https://api.compound.finance/api/v2/ctoken")
    df   = pd.DataFrame(columns = list(res.json()['cToken'][0].keys()))

    for ctoken in res.json()['cToken']:
        
        if ctoken['symbol'][1:]=='COMP' or ctoken['symbol'][1:]=='cWBTC':
            continue

        for key in ctoken.keys():
            try:
                df.at[ind,key ] = ctoken[key]['value']
            except:
                df.at[ind,key ] = ctoken[key]
                
        supply_ctokens   = float(df.at[ind,'total_supply'])
        supply_tokens    = supply_ctokens * float(df.at[ind,'exchange_rate'])
        
        borrow_tokens   = float(df.at[ind,'total_borrows'])
        
        try:
            response         = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names[ctoken['symbol'][1:]])+ "?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false").json()   
            current_price    = response['market_data']['current_price']['usd']
        except Exception as e:
            if 'BTC' in ctoken['symbol'][1:]:
                ctoken['symbol'] = 'cWBTC'
                response      = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names[ctoken['symbol'][1:]])+ "?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false").json()   
                current_price = response['market_data']['current_price']['usd']
            else:
                current_price     = 0.02  # for compound
            
            
        total_supply     = supply_tokens * current_price
        total_borrow     = borrow_tokens * current_price
        position         = total_borrow - total_supply
        
        df.at[ind,'total_supply' ] = total_supply
        df.at[ind,'total_borrows' ] = total_borrow
        df.at[ind,'supply_tokens' ] = supply_tokens
        df.at[ind,'borrow_tokens' ] = borrow_tokens
        
        df.at[ind,'position($)' ]  = position
        df.at[ind,'position(B_$)'] = position / pow(10,9)
        
        # print(total_supply, total_borrow, position)
        # print(ctoken['symbol'][1:], names[ctoken['symbol'][1:]] , current_price)
            
        if ctoken['symbol'][1:]=='cETH':    
            break
            
        ind += 1

    df['total_borrows'] = pd.to_numeric(df['total_borrows'])
    df['total_supply']  = pd.to_numeric(df['total_supply'])

    TOTAL_ASSETS      = df['total_borrows'].sum()
    TOTAL_LIABILITIES = df['total_supply'].sum()
    # response = requests.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false').json()
    response = requests.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false').json()
    for i in response:
        if i['symbol']=='comp':
            MARKET_CAP = i['market_cap']


    LCR = (MARKET_CAP * 0.7 ) / ( TOTAL_LIABILITIES* 0.2 + TOTAL_ASSETS* 0.1 )


    transactionResponse['response']['data']['COMPOUND']['TOTAL_LIABILITIES'].append(TOTAL_LIABILITIES)
    transactionResponse['response']['data']['COMPOUND']['TOTAL_ASSETS'].append(TOTAL_ASSETS)
    transactionResponse['response']['data']['COMPOUND']['MARKET_CAP'].append(MARKET_CAP)
    # transactionResponse['response']['data']['COMPOUND']['LCR'].append(LCR)
    transactionResponse['response']['result']['COMPOUND']['data']["total_liabilities"] = TOTAL_LIABILITIES
    transactionResponse['response']['result']['COMPOUND']['data']["total_assets"] = TOTAL_ASSETS
    transactionResponse['response']['result']['COMPOUND']['data']["market_cap"] = MARKET_CAP
    transactionResponse['response']['result']['COMPOUND']['data']['lcr'] = LCR

    return transactionResponse


def lambda_handler(event, context):
    print("Handler called")
    response = {}
    transactionResponse = {}
    try:
        # event           = json.loads(event)
        payload         = event["body"]
        transactionResponse  = main()
        

    except Exception as e:

        s = str(e)
        response["statusCode"] = 400
        transactionResponse["error"] = True
        transactionResponse["message"] = s
        response["headers"] = {}
        response["headers"]["Content-Type"] = "application/json"
        response["body"] = transactionResponse
        response_JSON = response
        return response_JSON

    response = transactionResponse
    return response

# if __name__ == "__main__":
#     input_data = {'body':{

#                         }
#                 }
#     # input_json = json.dumps(input_data)
#     result = lambda_handler(input_data,"Context")
#     print(result)
