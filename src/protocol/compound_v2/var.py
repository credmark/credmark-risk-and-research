## Imports
import os
import json
import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

#Formatted
names = {"DAI"  : 'dai',
        "SAI"   : 'sai',
        "REP"   : 'augur',
        "GUSD"  : 'gemini-dollar',
        "SUSD"  : 'susd',
        "TUSD"  : 'true-usd',
        "USDC"  : 'usd-coin',
        "USDP"  : 'usdp',
        "USDT"  : "tether",
        "BAL"   : "balancer",
        "ETH"   : "ethereum",
        "LINK"  : "chainlink",
        "MKR"   : "maker",
        "RAI"   : "rai",
        "UNI"   : "uniswap",
        "WBTC"  : "wrapped-bitcoin",
        "SUSHI":  "sushi",
        "YFI"   : "yearn-finance",
        "BUSD"  : "binance-usd",
        "FEI"   : "fei-usd",
        "FRAX"  : "frax",
        "AAVE"  : "aave",
        "AMPL"  : "ampleforth",
        "BAT"   : "basic-attention-token",
        "CRV"   : "curve-dao-token",
        "DPI"   : "defipulse-index",
        "ENJ"   : "enjin-coin",
        "KNC"   : "kyber-network-crystal",
        "MANA"  : "decentraland",
        "REN"   : "ren",
        "RENFIL": "renfil",
        "SNX"   :"synthetix-network-token",
        "ZRX"   : "0x"
        }

def change():
    columns = []
    columns.append('timestamp')
    for key in list(names.keys()):
        columns.append(key+'_price')
        columns.append(key+'_%change(10)')
    df        = pd.DataFrame(columns = columns)
    LOOK_BACK = 375 * 2
    #LIMIT     = 5
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
        P    = []
        if request.status_code != 200:
            data = {}
            data['prices'] = WBTC[ -LOOK_BACK: ]
            for day in data['prices']:
                P.append(day)
        else:   
            data = request.json()
            tt   = []
            

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
    
    print("PRICES FETECHED")
    
    unknown_assets = ['PAX']
    for asset in list(names.keys()):#[:LIMIT]:
        try:
            change = []
            diff_10 = pd.DataFrame(df[asset+'_price'].diff(periods=10) )
            
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
            
        except Exception as e:
            unknown_assets.append(asset)
            print("Error ", asset, e)
    
    for asset in unknown_assets:
        df[asset+'_%change(1)']  = df['WBTC_%change(1)'] 
        df[asset+'_%change(10)'] = df['WBTC_%change(10)'] 
    df['PAX_%change(10)'] = df['WBTC_%change(10)'] 
    df['PAX_%change(1)'] = df['WBTC_%change(1)'] 
        
    for key in track.keys():
        df[key+'_%change(10)'][:track[key]] = df['WBTC_%change(10)'][:track[key]]

    df['date'] = df['timestamp'].apply(lambda x : str(datetime.fromtimestamp(x).date()))
    df  = df.iloc[::-1]
    df  = df.reset_index()
    df = df.drop(['index'], axis=1)

    df = df[:-10]

    print("CHECK POINT ")
    return df

def main():

    transactionResponse = {"result":{"COMPOUND":{"data":{}, "context":{"errors":[]}}}}



    change_df = change()

    ind  = 0 
    res  = requests.get("https://api.compound.finance/api/v2/ctoken")
    df   = pd.DataFrame(columns = list(res.json()['cToken'][0].keys()))

    for ctoken in res.json()['cToken']:
        

        # Skipping COMP because of non-compatible data
        if (ctoken["underlying_symbol"] == "COMP"):
            continue

        ind += 1
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
            ctoken['symbol'] = 'cWBTC'
            
            if 'BTC' in ctoken['symbol'][1:]:
                response      = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names[ctoken['symbol'][1:]])+ "?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false").json()   
                current_price = response['market_data']['current_price']['usd']
        
            else:    
                current_price     = 0.02  # for compound
            
        
        total_supply     = supply_tokens * current_price
        total_borrow     = borrow_tokens * current_price
        position         = total_borrow  - total_supply
        
        df.at[ind,'total_supply' ] = total_supply



        df.at[ind,'total_borrow' ] = total_borrow
        df.at[ind,'position($)' ]  = position
        df.at[ind,'position(B_$)'] = position / pow(10,9)

        # print("Checking Individual Assets")
        # print(ctoken)
        # print(df.loc[ind]['total_supply'])
        # print(df.loc[ind]['total_borrow'])
        # print()
        
        # if ctoken['symbol'][1:]=='COMP':
        #     continue
        # else:
            # print(total_supply, total_borrow, position)
            # print(ctoken['symbol'][1:], names[ctoken['symbol'][1:]] , current_price)

    REQUIRED_1  = []
    REQUIRED_10 = []

    historicalVAR = pd.DataFrame(columns=['date', 'VAR_10', 'VAR_1'])
    hist_ind = 0


    for required_row in change_df.iterrows():
        required_row = required_row[1]
        SUM_10 = []
        SUM_1 = []
        for row in df.iterrows():
            if row[1]['underlying_symbol'] == 'COMP':
                continue
            SUM_10.append((float(row[1]['position(B_$)']) * float(required_row[ row[1]['underlying_symbol'] +'_%change(10)'])/100))
            SUM_1.append((float(row[1]['position(B_$)']) * float(required_row[ row[1]['underlying_symbol'] +'_%change(1)'])/100))

        REQUIRED_1.append(sum(SUM_1))
        REQUIRED_10.append(sum(SUM_10))
        historicalVAR.loc[hist_ind] = [required_row['date']] + [sum(SUM_10)] + [sum(SUM_1)]
        hist_ind += 1
        


    historicalVAR = historicalVAR[:365]


    # REQUIRED_10.sort()#(reverse=True)
    # REQUIRED_1.sort()#(reverse=True)
    REQUIRED_10 = historicalVAR.sort_values('VAR_10').iloc[3]
    REQUIRED_1 = historicalVAR.sort_values('VAR_1').iloc[3]

    # print("TESTING REQUIRED VALUES")
    # print(REQUIRED_10)
    # print("Historical Test")
    # print(historicalVAR.sort_values('VAR_10').head())
    # print()

    REQUIRED_10_95 = historicalVAR.sort_values('VAR_10').iloc[9]
    REQUIRED_1_95 = historicalVAR.sort_values('VAR_1').iloc[9]

    var_date_10_99p = REQUIRED_10['date']
    var_date_10_95p = REQUIRED_10_95['date']
    var_date_1_99p = REQUIRED_1['date']
    var_date_1_95p = REQUIRED_1_95['date']
    # response["statusCode"] = 200
    # print(REQUIRED_10[:10])
    # transactionResponse["result"]["AAVEV2"]["data"]['10_day_99p'] = str(REQUIRED_10[5])
    # transactionResponse["result"]["AAVEV2"]["data"]['10_day_99p'] = str(REQUIRED_10[4])
    transactionResponse["result"]["COMPOUND"]["data"]['10_day_99p'] = str(REQUIRED_10['VAR_10'])
    transactionResponse["result"]["COMPOUND"]["data"]['var_date_10_day_99p'] = var_date_10_99p

    transactionResponse["result"]["COMPOUND"]["data"]['10_day_95p'] = str(REQUIRED_10_95['VAR_10'])
    transactionResponse["result"]["COMPOUND"]["data"]['var_date_10_day_95p'] = var_date_10_95p

    # transactionResponse["result"]["AAVEV2"]["data"]['1_day_99p'] = str(REQUIRED_1[5])
    transactionResponse["result"]["COMPOUND"]["data"]['1_day_99p'] = str(REQUIRED_1['VAR_1'])
    transactionResponse["result"]["COMPOUND"]["data"]['var_date_1_day_99p'] = var_date_1_99p

    transactionResponse["result"]["COMPOUND"]["data"]['1_day_95p'] = str(REQUIRED_1_95['VAR_1'])
    transactionResponse["result"]["COMPOUND"]["data"]['var_date_1_day_95p'] = var_date_1_95p

    transactionResponse["result"]["COMPOUND"]["data"]['total_assets'] = float(df['total_borrow'].sum())
    transactionResponse["result"]["COMPOUND"]["data"]['total_liabilities'] = float(df['total_supply'].sum())
    transactionResponse["result"]["COMPOUND"]["data"]['relative_var_assets'] = str(REQUIRED_10['VAR_10'] * pow(10,9) /float(df['total_borrow'].sum()))
    transactionResponse["result"]["COMPOUND"]["data"]['relative_var_liabilities'] = str(REQUIRED_10['VAR_10'] * pow(10,9) /float(df['total_supply'].sum()))
    # REQUIRED_10.sort()
    # REQUIRED_1.sort()

    # transactionResponse["result"]["COMPOUND"]["data"]['10_day_99p'] = str(REQUIRED_10[4])
    # transactionResponse["result"]["COMPOUND"]["data"]['10_day_95p'] = str(REQUIRED_10[10])

    # transactionResponse["result"]["COMPOUND"]["data"]['1_day_99p'] = str(REQUIRED_1[4])
    # transactionResponse["result"]["COMPOUND"]["data"]['1_day_95p'] = str(REQUIRED_1[10])

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
#                         }
#                 }
#     input_json = json.dumps(input_data)
#     result = lambda_handler(input_data,"Context")
#     print(result)

