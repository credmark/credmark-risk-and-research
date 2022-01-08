# ## Imports
import os
import json
import boto3
import requests
import numpy as np
import pandas as pd
from io import StringIO
from datetime import timedelta
from datetime import datetime, timezone
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


def change():
    query = '''
                {
                reserves
                (where:{
                    symbol:"WBTC"
                    })
                {
                    id
                    symbol
                    name
                    usageAsCollateralEnabled
                        borrowingEnabled
                    baseLTVasCollateral
                    totalDeposits
                    totalLiquidity
                    totalATokenSupply
                    reserveFactor
                    
                }
                }
            '''

    V2_sample_transport       = RequestsHTTPTransport(
        url='https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
        verify=True,
        retries=5)
    V2_client                 = Client(transport=V2_sample_transport)
    response                  = V2_client.execute(gql(query))

    names = {"DAI"  : 'dai',
        "GUSD"  : 'gemini-dollar',
        # "SUSD"  : 'susd',
        "TUSD"  : 'true-usd',
        "USDC"  : 'usd-coin',
        "USDP"  : 'usdp',
        "USDT"  : "tether",
        "BAL"   : "balancer",
        "WETH"   : "ethereum",
        "LINK"  : "chainlink",
        "MKR"   : "maker",
        "RAI"   : "rai",
        "UNI"   : "uniswap",
        "WBTC"  : "wrapped-bitcoin",
        "XSUSHI": "xsushi",
        "YFI"   : "yearn-finance",
        "BUSD"  : "binance-usd",
        "FEI"   : "fei-usd",
        "FRAX"  : "frax",
        "AAVE"  : "aave",
        "AMPL"  : "ampleforth",
        "BAT"   : "basic-attention-token",
        "CRV"   : "curve-dao-token",
        "DPI"   : "defipulse-index",
        # "ENJ"   : "enjin-coin",
        "KNC"   : "kyber-network-crystal",
        "MANA"  : "decentraland",
        # "REN"   : "ren",
        "RENFIL": "renfil",
        # "SNX"   :"synthetix-network-token",
        "ZRX"   : "0x"
        }

    columns = []
    columns.append('timestamp')
    for key in list(names.keys()):
        columns.append(key+'_price')
        columns.append(key+'_%change(10)')


    df        = pd.DataFrame(columns = columns)
    LOOK_BACK = 375
    LIMIT     = 5
    WBTC      = []

    request   = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names['WBTC'])+ "/market_chart?vs_currency=usd&days=max&interval=daily")
    data      = request.json()
    WBTC      = data['prices']
    tt        = []
    P         = []
    for day in data['prices'][-LOOK_BACK:]:
        tt.append(day[0]/1000)
        P.append(day[1])
    df['WBTC_price'] = P
    df['timestamp']  = tt

    track = {}
    for asset in list(names.keys()):#[:LIMIT]:
        if asset == 'WBTC':
            continue
        request = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names[asset])+ "/market_chart?vs_currency=usd&days=max&interval=daily")
        data = request.json()
        tt = []
        P = []
        
        if len(data['prices']) < LOOK_BACK:
            req = LOOK_BACK - len(data['prices'])
            addition = WBTC[-LOOK_BACK:  -LOOK_BACK + req ]
            data['prices'] = addition + data['prices']
            track[asset] = req
        else:
            data['prices'] = data['prices'][-LOOK_BACK:]
        
        for day in data['prices']:
            tt.append(day[0]/1000)
            P.append(day[1])
        df[asset+'_price'] = P
        df['timestamp']  = tt

    df['timestamp'] = tt

    for asset in list(names.keys()):#[:LIMIT]:
        
        diff_10 = pd.DataFrame(df[asset+'_price'].diff(periods=10) )
        change = []
        for i in range(10):
            change.append('Nan')
        for i in range(10,df.shape[0]):
            change.append((diff_10.iloc[i][asset+'_price'] / df.iloc[i-10][asset+'_price']) * 100)
        df[asset+'_%change(10)'] = change
        
        diff_1 = pd.DataFrame(df[asset+'_price'].diff(periods=1) )
        change = []
        for i in range(10):
            change.append('Nan')
        for i in range(10,df.shape[0]):
            change.append((diff_1.iloc[i][asset+'_price'] / df.iloc[i-10][asset+'_price']) * 100)
        df[asset+'_%change(1)'] = change
        
    for key in track.keys():
        df[key+'_%change(10)'][:track[key]] = df['WBTC_%change(10)'][:track[key]]

    df['date'] = df['timestamp'].apply(lambda x : str(datetime.fromtimestamp(x).date()))
    df  = df.iloc[::-1]
    df  = df.reset_index()
    df = df.drop(['index'], axis=1)

        
    return df




def main(look_back=1, duration = 180):

    transactionResponse = {}

    names = {"DAI"  : 'dai',
            "GUSD"  : 'gemini-dollar',
            # "SUSD"  : 'susd',
            "TUSD"  : 'true-usd',
            "USDC"  : 'usd-coin',
            # "USDP"  : 'usdp',
            "USDT"  : "tether",
            "BAL"   : "balancer",
            "WETH"   : "ethereum",
            "LINK"  : "chainlink",
            "MKR"   : "maker",
            "RAI"   : "rai",
            "UNI"   : "uniswap",
            "WBTC"  : "wrapped-bitcoin",
            "XSUSHI": "xsushi",
            "YFI"   : "yearn-finance",
            "BUSD"  : "binance-usd",
            "FEI"   : "fei-usd",
            "FRAX"  : "frax",
            "AAVE"  : "aave",
            "AMPL"  : "ampleforth",
            "BAT"   : "basic-attention-token",
            "CRV"   : "curve-dao-token",
            "DPI"   : "defipulse-index",
            # # "ENJ"   : "enjin-coin",
            "KNC"   : "kyber-network-crystal",
            "MANA"  : "decentraland",
            # "REN"   : "ren",
            "RENFIL": "renfil",
            # "SNX"   :"synthetix-network-token",
            "ZRX"   : "0x"
            }

    errors = []
    # transactionResponse = {"statusCode":200,"response":{}}
    transactionResponse = {"result":{"AAVEV2":{"data":{}}}}
    ind       = 0
    look_back = 1
    query     = '''{reserves
        (where:{
            symbol:"%s"
        })
        {
            name
            symbol
                reserveFactor
            decimals
            utilizationRate
            availableLiquidity
            totalLiquidity
            reserveFactor
        }}'''

    df1 = pd.DataFrame(columns=['Asset', 'CurrentPrice', 'availableLiquidity','decimals', 'totalLiquidity','utilizationRate'])
    for asset in names.keys():
        try:
            print(names[asset])
            request = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names[asset]) + "/market_chart?vs_currency=usd&days="+ str(look_back) +"&interval=daily")
            data1   = request.json()
            price   =  data1['prices'][0][1]
            
            V2_sample_transport       = RequestsHTTPTransport(
                url='https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
                verify=True,
                retries=5)
            V2_client                 = Client(transport=V2_sample_transport)
            response                  = V2_client.execute(gql(query % asset ))['reserves'][0]
            
            df1.loc[ind] = [asset] + [price] + [float(response['availableLiquidity'])] + [response['decimals']]+ \
                                    [float(response['totalLiquidity'])]+ [float(response['utilizationRate'])]
            ind += 1

        except Exception as e:
            print("**", asset, e) #str(asset), str(price_mean), str(price_std), str(cap_mean),str(cap_std), str(vol_mean) , str(vol_std))  
            errors.append(asset)
            # transactionResponse["error"] = True
        transactionResponse["result"]["AAVEV2"]["context"] = {}
        transactionResponse["result"]["AAVEV2"]["context"]["errors"] = errors


    df1['availableLiquidity']       = pd.to_numeric(df1['availableLiquidity'])
    df1['utilizationRate']          = pd.to_numeric(df1['utilizationRate'])
    df1['decimals']                 = pd.to_numeric(df1['decimals'])
    df1['CurrentPrice']             = pd.to_numeric(df1['CurrentPrice'])
    df1['MARKET']                   = (df1['availableLiquidity'] / (1 - df1['utilizationRate']))
    df1['MARKET']                   = df1['MARKET'] / pow(10, df1['decimals'])
    df1['totalBorrow']              = df1['MARKET']  * df1['utilizationRate'] 
    df1['totalBorrow']              = df1['totalBorrow'] / pow(10, df1['decimals'])
    df1['availableLiquidity']       = df1['availableLiquidity'] / pow(10, df1['decimals'])
    df1['position']                 = df1['totalBorrow'] - df1['MARKET']  # df1['availableLiquidity']
    df1['Asset($)']                 = df1['CurrentPrice'] * df1['position']
    df1['Asset($,Billion)']         = df1['Asset($)'] / pow(10, 9)

    # TOTAL_ASSETS = df1['totalBorrow'].sum()
    # TOTAL_LIABILITIES = df1['MARKET'].sum()


    df = change()


    REQUIRED_1 = []
    REQUIRED_10 = []
    historicalVAR = pd.DataFrame(columns=['date', 'VAR_10', 'VAR_1'])
    hist_ind = 0
    for required_row in df.iterrows():
        required_row = required_row[1]
        SUM_10 = []
        SUM_1 = []
        for row in df1.iterrows():
            SUM_10.append((row[1]['Asset($,Billion)'] * float(required_row[ row[1]['Asset'] +'_%change(10)'])/100))
            SUM_1.append((row[1]['Asset($,Billion)'] * float(required_row[ row[1]['Asset'] +'_%change(1)'])/100))
        
        REQUIRED_1.append(sum(SUM_1))
        REQUIRED_10.append(sum(SUM_10))
        
        historicalVAR.loc[hist_ind] = [required_row['date']] + [sum(SUM_10)] + [sum(SUM_1)]
        hist_ind += 1
    
    print("DEBUG ", historicalVAR.shape)

    # ALL_TIME_VAR_10 = historicalVAR['VAR_10'].min()
    # ALL_TIME_VAR_1 = historicalVAR['VAR_1'].min()
    # ALL_TIME_VAR_10_DATE = historicalVAR['date'][historicalVAR['VAR_10'].idxmin()]
    # ALL_TIME_VAR_1_DATE = historicalVAR['date'][historicalVAR['VAR_1'].idxmin()]

    # REQUIRED_10 = REQUIRED_10[:365]
    # REQUIRED_1 = REQUIRED_1[:365]

    historicalVAR = historicalVAR[:365]

    
    # REQUIRED_10.sort()#(reverse=True)
    # REQUIRED_1.sort()#(reverse=True)
    REQUIRED_10 = historicalVAR.sort_values('VAR_10').iloc[3]
    REQUIRED_1 = historicalVAR.sort_values('VAR_1').iloc[3]

    REQUIRED_10_95 = historicalVAR.sort_values('VAR_10').iloc[9]
    REQUIRED_1_95 = historicalVAR.sort_values('VAR_1').iloc[9]

    var_date_10_99p = REQUIRED_10['date']
    var_date_10_95p = REQUIRED_10_95['date']
    var_date_1_99p = REQUIRED_1['date']
    var_date_1_95p = REQUIRED_1_95['date']


    # dt = datetime.now()
    # dt = dt.replace(tzinfo=timezone.utc)
    # print(dt)
    current_date = str(datetime.now().date().strftime("%m-%d-%Y"))
    
    print('https://aave-api-v2.aave.com/data/liquidity/v2?poolId=0xb53c1a33016b2dc2ff3653530bff1848a515c8c5&date='+current_date)
    res = requests.get('https://aave-api-v2.aave.com/data/liquidity/v2?poolId=0xb53c1a33016b2dc2ff3653530bff1848a515c8c5&date='+current_date).json() #12-14-2021'
    print("Print Test Res")
    print(res)

    df = pd.DataFrame(columns=['SYMBOL', 'totalLiquidity', 'totalDebt'])
    ind = 0
    for i in res:
        df.loc[ind] = [i['symbol']] + [float(i['totalLiquidity']) * float(i['referenceItem']['priceInUsd']) ]\
                    + [float(i['totalDebt']) * float(i['referenceItem']['priceInUsd'] )] #+ [i['referenceItem']['priceInUsd'] * ]
        ind += 1

    TOTAL_LIABILITIES = df['totalLiquidity'].sum()
    TOTAL_ASSETS      = df['totalDebt'].sum()


    # response["statusCode"] = 200
    # print(REQUIRED_10[:10])
    # transactionResponse["result"]["AAVEV2"]["data"]['10_day_99p'] = str(REQUIRED_10[5])
    # transactionResponse["result"]["AAVEV2"]["data"]['10_day_99p'] = str(REQUIRED_10[4])
    transactionResponse["result"]["AAVEV2"]["data"]['10_day_99p'] = str(REQUIRED_10['VAR_10'])
    transactionResponse["result"]["AAVEV2"]["data"]['var_date_10_day_99p'] = var_date_10_99p

    transactionResponse["result"]["AAVEV2"]["data"]['10_day_95p'] = str(REQUIRED_10_95['VAR_10'])
    transactionResponse["result"]["AAVEV2"]["data"]['var_date_10_day_95p'] = var_date_10_95p

    # transactionResponse["result"]["AAVEV2"]["data"]['1_day_99p'] = str(REQUIRED_1[5])
    transactionResponse["result"]["AAVEV2"]["data"]['1_day_99p'] = str(REQUIRED_1['VAR_1'])
    transactionResponse["result"]["AAVEV2"]["data"]['var_date_1_day_99p'] = var_date_1_99p

    transactionResponse["result"]["AAVEV2"]["data"]['1_day_95p'] = str(REQUIRED_1_95['VAR_1'])
    transactionResponse["result"]["AAVEV2"]["data"]['var_date_1_day_95p'] = var_date_1_95p

    transactionResponse["result"]["AAVEV2"]["data"]['relative_var_assets'] = str(REQUIRED_10['VAR_10'] * pow(10,9) / TOTAL_ASSETS)
    transactionResponse["result"]["AAVEV2"]["data"]['relative_var_liabilities'] = str(REQUIRED_10['VAR_10'] * pow(10,9) / TOTAL_LIABILITIES)
    transactionResponse["result"]["AAVEV2"]["data"]['total_assets'] = TOTAL_ASSETS
    transactionResponse["result"]["AAVEV2"]["data"]['total_liabilities'] = TOTAL_LIABILITIES

    


    return  transactionResponse


def lambda_handler(event, context):
    print("Handler called")
    response = {}
    transactionResponse = {}
    try:
        # event           = json.loads(event)
        payload         = event["body"]
        transactionResponse  = main(payload['look_back'], payload['duration'])
        

    except Exception as e:

        s = str(e)
        response["statusCode"] = 400
        transactionResponse["error"] = True
        transactionResponse["message"] = s
        response["headers"] = {}
        response["headers"]["Content-Type"] = "application/json"
        response["response"] = transactionResponse
        response_JSON = response
        return response_JSON

    response["statusCode"] = 200
    transactionResponse["error"] = False
    transactionResponse["message"] = "Error free execution"
    response["headers"] = {}
    response["headers"]["Content-Type"] = "application/json"
    response["response"] = transactionResponse
    response_JSON = response
    return response_JSON


# if __name__ == "__main__":
#     input_data = {'body':{
#         'duration' : 180,
#         'look_back':1

#                         }
#                 }
#     input_json = json.dumps(input_data)
#     result = lambda_handler(input_data,"Context")
#     print(result)

