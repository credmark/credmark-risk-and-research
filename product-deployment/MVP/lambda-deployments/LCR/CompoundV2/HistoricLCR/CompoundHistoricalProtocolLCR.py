import requests
import pandas as pd
import numpy as np 
from datetime import timedelta
from datetime import datetime

# Formatted

def main(date):
    # "d-m-y"
    FROM_DATETIME  = datetime.strptime(date,'%d-%m-%Y')
    transactionResponse = {"result":{}}
    if (datetime.now() - FROM_DATETIME).days < 0:
        
        transactionResponse['result']        = "Date greater than current date"
        print("CHECKing ------  ")
        return transactionResponse


    if (datetime.now() - FROM_DATETIME).days > 0 and ((datetime.now() - FROM_DATETIME).days ) < 5:
        CHECK = (datetime.now() - FROM_DATETIME).days

    else:
        CHECK = 5

    historical_market_cap_df = pd.DataFrame(columns=['timestamp', 'CAP', 'date'])
    ind = 0
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
            #"COMP"  : "compound"
        }

    response = requests.get('https://api.coingecko.com/api/v3/coins/ethereum/contract/0xc00e94cb662c3520282e6f5717214004a7f26888/market_chart/?vs_currency=usd&days=max').json()   

    for resp in response['market_caps']:
        historical_market_cap_df.at[ind, 'timestamp'] = float(resp[0])/1000
        historical_market_cap_df.at[ind, 'CAP']       = float(resp[1])
        date = datetime.fromtimestamp(float(resp[0])/1000)
        historical_market_cap_df.at[ind, 'date']      = str(date.date())
        ind += 1
    historical_market_cap_df = historical_market_cap_df.iloc[::-1]

    asset_address_dict = {
        "cREP"  : "0x158079ee67fce2f58472a96584a73c7ab9ac95c1",
        "cTUSD"  : "0x12392f67bdf24fae0af363c24ac620a2f67dad86",
        "cAAVE"  : "0xe65cdb6479bac1e22340e4e755fae7e509ecd06c",
        "cSAI"  : "0xf5dce57282a584d2746faf1593d3121fcac444dc",
        "cZRX"  : "0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407",
        "cYFI"  : "0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946",
        "cWBTC"  : "0xc11b1268c1a384e55c48c2391d8d480264a3a7f4",
        "cMKR"  : "0x95b4ef2869ebd94beb4eee400a99824bf5dc325b",
        "cUNI"  : "0x35a18000230da775cac24873d00ff85bccded550",
        "cBAT"  : "0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e",
        "cCOMP"  : "0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4",
        "cLINK"  : "0xface851a4921ce59e912d19329929ce6da6eb0c7",
        "cETH"  : "0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5",
        "cWBTC2" : "0xccf4429db6322d5c611ee964527d42e5d685dd6a",
        "cUSDT"  : "0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9",
        "cUSDC"  : "0x39aa39c021dfbae8fac545936693ac917d5e7563",
        "cDAI"  : "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
        "cSUSHI" : "0x4b0181102a0112a2ef11abee5563bb4a3176c9d7"
    }
    exchange_dict = {}
    res  = requests.get("https://api.compound.finance/api/v2/ctoken")
    for ctoken in res.json()['cToken']:
        exchange_dict[ ctoken['symbol']] =  ctoken['exchange_rate']


    # REQUIRED_DATE = '02-12-2021'
    # REQUIRED_DATE = date
    # FROM_DATETIME = datetime.strptime(REQUIRED_DATE,'%d-%m-%Y')
    DELTA = 1
    result = []
    for _ in range(CHECK):
        res_temp = {}
        df  = pd.DataFrame(columns = ['Asset', "supply_mean", "borrow_mean", 'name'])
        ind = 0
        CURRENT_DATETIME = FROM_DATETIME + timedelta(DELTA)
        for key in asset_address_dict.keys():
            ASSET = asset_address_dict[key]
            res   = requests.get("https://api.compound.finance/api/v2/market_history/graph?asset=%s&min_block_timestamp=%s&max_block_timestamp=%s&num_buckets=200" % (ASSET,str(int(FROM_DATETIME.timestamp())), str(int(CURRENT_DATETIME.timestamp())))).json()      
            
            df.at[ind, "Asset"] = res['asset']
            df.at[ind, "name"]  = key
            borrow_list = []
            if len(res['total_borrows_history'])>0:
                for val in res['total_borrows_history']:
                    borrow_list.append(val['total']['value'])
                borrow_mean = np.asarray(borrow_list).astype(np.float32).mean()
            else:
                borrow_mean = 0
                
            #multiply borrow with underlying token price (in $ ) as they are underlying token

            supply_list = []
            if len(res['total_supply_history'])>0:
                for val in res['total_supply_history']:
                    supply_list.append(val['total']['value'])
                supply_mean = np.asarray(supply_list).astype(np.float32).mean()
            
            else:
                supply_mean = 0
                
            #price multiplication
            
            supply_ctokens   = supply_mean# 79074216.63146698
            supply_tokens    = float(supply_ctokens) * float(exchange_dict[key]['value'])
            borrow_tokens   = borrow_mean

            try:
                response         = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names[key[1:]])+ "?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false").json()   
                current_price    = response['market_data']['current_price']['usd']
            except Exception as e: 
                if key == 'cWBTC2':
                    key = 'cWBTC'
                    response      = requests.get("https://api.coingecko.com/api/v3/coins/"+ str(names[key[1:]])+ "?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false").json()   
                    current_price = response['market_data']['current_price']['usd']
                else:
                    current_price     = 0.02  # for compound
            
            # Multiply supply ctokens with exchange rate as they are ctokens and then multiply with $ price
            
            total_supply     = supply_tokens * current_price
            total_borrow     = borrow_tokens * current_price
            
            df.at[ind, "supply_mean"]  = total_supply
            df.at[ind, "borrow_mean"]  = total_borrow
            ind += 1        
        TOTAL_LIABILITIES = df['supply_mean'].sum()
        TOTAL_ASSETS      = df['borrow_mean'].sum()
        req = historical_market_cap_df[historical_market_cap_df['timestamp']>=FROM_DATETIME.timestamp()]
        req = req[req['timestamp']<=CURRENT_DATETIME.timestamp()]
        MARKET_CAP = req['CAP'].mean()
        
        LCR = (MARKET_CAP * 0.7 ) / ( TOTAL_LIABILITIES* 0.2 + TOTAL_ASSETS* 0.1 )
        
        print("LCR ", LCR)

        res_temp['data'] = {
            'timestamp'        :FROM_DATETIME.timestamp(),
            'date'             :str(FROM_DATETIME.date()),
            'lcr'              :LCR,
            'market_cap'       :MARKET_CAP,
            'total_assets'     :TOTAL_ASSETS,
            'total_liabilities':TOTAL_LIABILITIES
        }

        result.append(res_temp)
        
        FROM_DATETIME = FROM_DATETIME + timedelta(DELTA)

    transactionResponse["result"]["COMPOUND"]= result


    return transactionResponse





def lambda_handler(event, context):
    print("Handler called")
    response = {}
    transactionResponse = {}
    try:
        # event              = json.loads(event)
        payload              = event["body"]
        date                 = payload['date']
        print("CHECK")
        transactionResponse  = main(date)
        

    except Exception as e:

        s = str(e)
        response["statusCode"]              = 400
        transactionResponse["error"]        = True
        transactionResponse["message"]      = s
        response["headers"]                 = {}
        response["headers"]["Content-Type"] = "application/json"
        response["response"]                = transactionResponse
        response_JSON                       = response
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
#                             'date':'05-12-2021'
#                          }
#                 }
#     # input_json = json.dumps(input_data)
#     result = lambda_handler(input_data,"Context")
#     print(result)

