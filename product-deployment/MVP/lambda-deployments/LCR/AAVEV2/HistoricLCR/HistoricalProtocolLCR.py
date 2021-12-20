
import os
import json
import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

graph_base_query = '''    
    {
  reserves 
      (where: 
        {symbol:"%s"}) {
        decimals
        paramsHistory( orderBy: timestamp, orderDirection:desc,
        where:
          {
            timestamp_gt:%s,
            timestamp_lt:%s
          }  
            ) {
        
          variableBorrowRate
          utilizationRate
          liquidityRate
          timestamp
          availableLiquidity
          lifetimeBorrows
          lifetimeDepositorsInterestEarned
          variableBorrowRate
          stableBorrowRate
        }
      }
    }
    
    

    '''


class TotalMarketExtractor():
    
    def __init__(self):
        
        self.OUTPUT                     = {'status':200,'response':{
                                            'data':{'V2':[], 'V1':[],'MATIC':[], 'AVALANCHE':[]},
                                            'result':{'Assets':[],'LCR':[], 'V2_RATIO':[] }}} 
        self.errors                     = [] 
        self.AAVE_MARKET_CAP            = 0
        self.V2_Assets                  = 0
        self.V2_Liabilities             = 0
        self.V2_LIABILITIES_SUM         = 0
        self.V2_Assets_SUM              = 0
        self.TOKEN_DATA                 = {}     
        self.graph_token_query          = '''
                                query {

                                    reserves{
                                    symbol
                                  }
                                }
                                '''
        self.graph_base_query           = '''
                             {
                                  reserves 
                                      (where: 
                                        {symbol:"%s"}) {
                                        decimals
                                        paramsHistory( orderBy: timestamp, orderDirection:desc,
                                        where:
                                          {
                                            timestamp_gt:%s,
                                            timestamp_lt:%s
                                          }  
                                            ) {

                                          variableBorrowRate
                                          utilizationRate
                                          liquidityRate
                                          timestamp
                                          availableLiquidity
                                          lifetimeBorrows
                                          lifetimeDepositorsInterestEarned
                                          variableBorrowRate
                                          stableBorrowRate
                                        }
                                      }
                                    }
                                '''
        
        V2_sample_transport             = RequestsHTTPTransport(
           url='https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
           verify=True,
           retries=5)
        self.V2_client                  = Client(transport=V2_sample_transport)
        V1_sample_transport             = RequestsHTTPTransport(
                   url='https://api.thegraph.com/subgraphs/name/aave/protocol-multy-raw',
                   verify=True,
                   retries=5)     
        self.V1_client                  = Client(transport=V1_sample_transport)
        MATIC_sample_transport          = RequestsHTTPTransport(
                   url='https://api.thegraph.com/subgraphs/name/aave/aave-v2-matic',
                   verify=True,
                   retries=5)    
        self.MATIC_client               = Client(transport=MATIC_sample_transport)
        Avalanche_sample_transport      = RequestsHTTPTransport(
                   url='https://api.thegraph.com/subgraphs/name/aave/protocol-v2-avalanche',
                   verify=True,
                   retries=5,)

        self.Avalanche_client           = Client(transport=Avalanche_sample_transport)
        
        self.RAY                        = pow(10, 27)
        self.AMM_Tokens                 = []
        
        
    def V2_update(self):
        response   = self.V2_client.execute(gql(self.graph_token_query))
        V2_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol']:
                V2_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
                
        return V2_tokens
                
    def V1_update(self):
        
        response   = self.V1_client.execute(gql(self.graph_token_query))
        V1_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol']:
                V1_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
                
        return V1_tokens
    
    def MATIC_update(self):
        
        response     = self.MATIC_client.execute(gql(self.graph_token_query))
        MATIC_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol']:
                MATIC_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
                
        return MATIC_tokens
    
    def AVALANCHE_update(self):
        
        response         = self.Avalanche_client.execute(gql(self.graph_token_query))
        AVALANCHE_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol']:
                AVALANCHE_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
            
        return AVALANCHE_tokens

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


    print("CHECKPOINT ", CHECK)

    market           = TotalMarketExtractor()
    V2_tokens        = market.V2_update()
    V1_tokens        = market.V1_update()
    Matic_tokens     = market.MATIC_update()
    AVALANCHE_tokens = market.AVALANCHE_update()
    CURRENT_DATETIME = FROM_DATETIME + timedelta(1)

    

    RAY = pow(10, 27)
    LCR = []
    
    res = []
    for _ in range(CHECK):
        print(FROM_DATETIME,CURRENT_DATETIME )
        res_temp                  = {}
        V2_Assets_SUM             = 0
        V2_LIABILITIES_SUM        = 0
        V1_Assets_SUM             = 0
        V1_LIABILITIES_SUM        = 0
        MATIC_Assets_SUM          = 0
        MATIC_LIABILITIES_SUM     = 0
        Avalanche_Assets_SUM      = 0
        Avalanche_LIABILITIES_SUM = 0
        
        for token in V2_tokens:
            responseV2       = market.V2_client.execute(gql(market.graph_base_query % (token, str(int(FROM_DATETIME.timestamp())) , str(int(CURRENT_DATETIME.timestamp())))))
            dfV2        = pd.DataFrame(columns=['availableLiquidity','lifetimeBorrows','lifetimeDepositorsInterestEarned','liquidityRate','stableBorrowRate','timestamp','utilizationRate','variableBorrowRate'])
            ind       = 0
            DECIMAL   = float(responseV2['reserves'][0]['decimals'])
            for  row in responseV2['reserves'][0]['paramsHistory']:
                values = list(row.values())
                dfV2.loc[ind] =  [float(values[0])] + [float(values[1])]  + [float(values[2])]  + [float(values[3])]  + [float(values[4])]  + [float(values[5])]  + [float(values[6])] + [float(values[7])]  
                ind += 1
                
            if dfV2.shape[0]<1:
                continue
            dfV2_mean = dfV2.mean()
            
            ITER_ = {}
            ITER  = {}
            
            ITER['liquidityRate']            = dfV2_mean['liquidityRate']
            ITER['variableBorrowRate']       = dfV2_mean['variableBorrowRate']
            ITER['stableBorrowRate']         = dfV2_mean['stableBorrowRate']
            ITER['utilizationRate']          = dfV2_mean['utilizationRate']
            ITER['DECIMAL']                  = DECIMAL #dfV2_mean['decimals']
            ITER['availableLiquidity']       = dfV2_mean['availableLiquidity']  / pow( 10 , ITER['DECIMAL'])
            ITER['MARKET']                   = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
            ITER['totalBorrow']              = ITER['MARKET']  * ITER['utilizationRate'] 
            ITER['percentDepositAPY']        = round(100 * ITER['liquidityRate']/RAY,      5)
            ITER['percentVariableBorrowAPY'] = round(100 * ITER['variableBorrowRate']/RAY, 5)
            ITER['percentStableBorrowAPY']   = round(100 * ITER['stableBorrowRate']/RAY,  10)
            V2_LIABILITIES_SUM               += ITER['MARKET']
            V2_Assets_SUM                    += ITER['totalBorrow']
        for token in V1_tokens:    
            responseV1  = market.V1_client.execute(gql(market.graph_base_query % (token, str(int(FROM_DATETIME.timestamp())) , str(int(CURRENT_DATETIME.timestamp())))))
            dfV1        = pd.DataFrame(columns=['availableLiquidity','lifetimeBorrows','lifetimeDepositorsInterestEarned','liquidityRate','stableBorrowRate','timestamp','utilizationRate','variableBorrowRate'])
            ind       = 0
            DECIMAL   = float(responseV1['reserves'][0]['decimals'])
            if len(responseV1['reserves'][0]['paramsHistory']) < 1:
                continue
            for  row in responseV1['reserves'][0]['paramsHistory']:
                values = list(row.values())
                dfV1.loc[ind] =  [float(values[0])] + [float(values[1])]  + [float(values[2])]  + [float(values[3])]  + [float(values[4])]  + [float(values[5])]  + [float(values[6])] + [float(values[7])]  
                ind += 1
            
            if dfV1.shape[0]<1:
                continue
            
            dfV1_mean = dfV1.mean()
            ITER_ = {}
            ITER  = {}
            ITER['liquidityRate']            = dfV1_mean['liquidityRate']
            ITER['variableBorrowRate']       = dfV1_mean['variableBorrowRate']
            ITER['stableBorrowRate']         = dfV1_mean['stableBorrowRate']
            ITER['utilizationRate']          = dfV1_mean['utilizationRate']
            ITER['DECIMAL']                  = DECIMAL#dfV1_mean['decimals']
            ITER['availableLiquidity']       = dfV1_mean['availableLiquidity']  / pow( 10 , ITER['DECIMAL'])
            ITER['MARKET']                   = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
            ITER['totalBorrow']              = ITER['MARKET']  * ITER['utilizationRate'] 
            ITER['percentDepositAPY']        = round(100 * ITER['liquidityRate']/RAY,      5)
            ITER['percentVariableBorrowAPY'] = round(100 * ITER['variableBorrowRate']/RAY, 5)
            ITER['percentStableBorrowAPY']   = round(100 * ITER['stableBorrowRate']/RAY,  10)
            V1_Assets_SUM                    += ITER['totalBorrow']
            V1_LIABILITIES_SUM               += ITER['MARKET']
        for token in Matic_tokens:    
            responseMatic  = market.MATIC_client.execute(gql(market.graph_base_query % (token, str(int(FROM_DATETIME.timestamp())) , str(int(CURRENT_DATETIME.timestamp())))))
            dfMatic     = pd.DataFrame(columns=['availableLiquidity','lifetimeBorrows','lifetimeDepositorsInterestEarned','liquidityRate','stableBorrowRate','timestamp','utilizationRate','variableBorrowRate'])
            ind         = 0
            DECIMAL     = float(responseV1['reserves'][0]['decimals'])
            if len(responseMatic['reserves'][0]['paramsHistory']) < 1:
                continue
            for  row in responseMatic['reserves'][0]['paramsHistory']:
                values = list(row.values())
                dfMatic.loc[ind] =  [float(values[0])] + [float(values[1])]  + [float(values[2])]  + [float(values[3])]  + [float(values[4])]  + [float(values[5])]  + [float(values[6])] + [float(values[7])]  
                ind += 1
            
            if dfMatic.shape[0]<1:
                continue
            dfMatic_mean = dfMatic.mean()
            ITER_ = {}
            ITER  = {}
            ITER['liquidityRate']            = dfMatic_mean['liquidityRate']
            ITER['variableBorrowRate']       = dfMatic_mean['variableBorrowRate']
            ITER['stableBorrowRate']         = dfMatic_mean['stableBorrowRate']
            ITER['utilizationRate']          = dfMatic_mean['utilizationRate']
            ITER['DECIMAL']                  = DECIMAL#dfV1_mean['decimals']
            ITER['availableLiquidity']       = dfMatic_mean['availableLiquidity']  / pow( 10 , ITER['DECIMAL'])
            ITER['MARKET']                   = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
            ITER['totalBorrow']              = ITER['MARKET']  * ITER['utilizationRate'] 
            ITER['percentDepositAPY']        = round(100 * ITER['liquidityRate']/RAY,      5)
            ITER['percentVariableBorrowAPY'] = round(100 * ITER['variableBorrowRate']/RAY, 5)
            ITER['percentStableBorrowAPY']   = round(100 * ITER['stableBorrowRate']/RAY,  10)
            MATIC_Assets_SUM                 += ITER['totalBorrow']
            MATIC_LIABILITIES_SUM            += ITER['MARKET']
        for token in AVALANCHE_tokens:    
            responseAVALANCHE  = market.Avalanche_client.execute(gql(market.graph_base_query % (token, str(int(FROM_DATETIME.timestamp())) , str(int(CURRENT_DATETIME.timestamp())))))
            dfAVALANCHE     = pd.DataFrame(columns=['availableLiquidity','lifetimeBorrows','lifetimeDepositorsInterestEarned','liquidityRate','stableBorrowRate','timestamp','utilizationRate','variableBorrowRate'])
            ind         = 0
            DECIMAL     = float(responseV1['reserves'][0]['decimals'])
            if len(responseAVALANCHE['reserves'][0]['paramsHistory']) < 1:
                continue
            for  row in responseAVALANCHE['reserves'][0]['paramsHistory']:
                values = list(row.values())
                dfAVALANCHE.loc[ind] =  [float(values[0])] + [float(values[1])]  + [float(values[2])]  + [float(values[3])]  + [float(values[4])]  + [float(values[5])]  + [float(values[6])] + [float(values[7])]  
                ind += 1
            
            if dfAVALANCHE.shape[0]<1:
                continue
            
            dfAVALANCHE_mean = dfAVALANCHE.mean()
            ITER_ = {}
            ITER  = {}
            ITER['liquidityRate']            = dfAVALANCHE_mean['liquidityRate']
            ITER['variableBorrowRate']       = dfAVALANCHE_mean['variableBorrowRate']
            ITER['stableBorrowRate']         = dfAVALANCHE_mean['stableBorrowRate']
            ITER['utilizationRate']          = dfAVALANCHE_mean['utilizationRate']
            ITER['DECIMAL']                  = DECIMAL#dfV1_mean['decimals']
            ITER['availableLiquidity']       = dfAVALANCHE_mean['availableLiquidity']  / pow( 10 , ITER['DECIMAL'])
            ITER['MARKET']                   = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
            ITER['totalBorrow']              = ITER['MARKET']  * ITER['utilizationRate'] 
            ITER['percentDepositAPY']        = round(100 * ITER['liquidityRate']/RAY,      5)
            ITER['percentVariableBorrowAPY'] = round(100 * ITER['variableBorrowRate']/RAY, 5)
            ITER['percentStableBorrowAPY']   = round(100 * ITER['stableBorrowRate']/RAY,  10)
            Avalanche_Assets_SUM             += ITER['totalBorrow']
            Avalanche_LIABILITIES_SUM        += ITER['MARKET']  

        # print("V2_Assets_SUM",V2_Assets_SUM)
        # print("V1_Assets_SUM",V1_Assets_SUM)
        # print("MATIC_Assets_SUM", MATIC_Assets_SUM)
        # print("Avalanche_Assets_SUM", Avalanche_Assets_SUM)
        

        V2_LIABILITIES_SUM        = V2_LIABILITIES_SUM        / len(V2_tokens)
        MATIC_LIABILITIES_SUM     = MATIC_LIABILITIES_SUM     / len(Matic_tokens)
        Avalanche_LIABILITIES_SUM = Avalanche_LIABILITIES_SUM / len(AVALANCHE_tokens)
        V1_LIABILITIES_SUM        = V1_LIABILITIES_SUM        / len(V1_tokens)

        RATIO = V2_LIABILITIES_SUM / (V2_LIABILITIES_SUM + MATIC_LIABILITIES_SUM + Avalanche_LIABILITIES_SUM + V1_LIABILITIES_SUM )
        V2_LIABILITIES_SUM        = V2_LIABILITIES_SUM        * len(V2_tokens)
         
        
        ddate = str(datetime.strftime(CURRENT_DATETIME, "%d-%m-%Y"))
        response1 = requests.get('https://api.coingecko.com/api/v3/coins/aave/history?date='+date+'&localization=false').json()['market_data']['market_cap']['usd']      
        date = str(datetime.strftime(FROM_DATETIME, "%d-%m-%Y"))
        response2 = requests.get('https://api.coingecko.com/api/v3/coins/aave/history?date='+date+'&localization=false').json()['market_data']['market_cap']['usd']      
        # print("DEBUG *****")
        # print(response1, response2)
        AAVE_MARKET_CAP = (response1 + response2) / 2

        # print("DEBUG ")
        # print("Libilities ", V2_LIABILITIES_SUM)
        # print("Assets " ,    V2_Assets_SUM )
        # print("Market CAP ", AAVE_MARKET_CAP) 

        lcr = (float(AAVE_MARKET_CAP) * float(RATIO) * 0.3 )  /  (float(V2_LIABILITIES_SUM) * 0.2 +  float(V2_Assets_SUM) * 0.1 ) 
        
        res_temp['data'] = {
            'timestamp'        :CURRENT_DATETIME.timestamp(),
            'date'             :str(CURRENT_DATETIME.date()),
            'lcr'              :lcr,
            'v2_ratio'         :RATIO,
            'market_cap'       :AAVE_MARKET_CAP,
            'total_assets'     :V2_Assets_SUM,
            'total_liabilities':V2_LIABILITIES_SUM
        }
        
        res.append(res_temp)
        FROM_DATETIME = CURRENT_DATETIME
        CURRENT_DATETIME = CURRENT_DATETIME + timedelta(1)

    transactionResponse["result"]["AAVEV2"]= res

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


if __name__ == "__main__":
    input_data = {'body':{
                            'date':'05-12-2021'
                         }
                }
    # input_json = json.dumps(input_data)
    result = lambda_handler(input_data,"Context")
    print(result)

