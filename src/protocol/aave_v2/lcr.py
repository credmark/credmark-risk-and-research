## Imports
import os
import json
import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


class TotalMarketExtractor():
    
    def __init__(self):
        
        self.OUTPUT                     = {'status':200,'response':{
                                            'data':{'AAVEV2':[], 'V1':[],'MATIC':[], 'AVALANCHE':[]},
                                            'result':{'AAVEV2':{'data':{'lcr':[], 'v2_ratio':[] }}}}}
        self.errors                     = []
    
        self.AAVE_MARKET_CAP            = 0
        self.V2_Assets                  = 0
        self.V1_Assets                  = 0
        self.MATIC_Assets               = 0
        self.Avalanche_Assets           = 0
        self.V2_Liabilities             = 0
        self.V1_Liabilities             = 0
        self.MATIC_Liabilities          = 0
        self.Avalanche_Liabilities      = 0 
        
        self.V2_LIABILITIES_SUM         = 0
        self.V1_LIABILITIES_SUM         = 0
        self.MATIC_LIABILITIES_SUM      = 0
        self.Avalanche_LIABILITIES_SUM  = 0
        
        self.V2_Assets_SUM              = 0
        self.V1_Assets_SUM              = 0
        self.MATIC_Assets_SUM           = 0
        self.Avalanche_Assets_SUM       = 0
        
        self.TOKEN_DATA = {}
        
        self.graph_token_query          = '''
                                query {

                                    reserves{
                                    symbol
                                  }
                                }
                                '''
        self.graph_base_query           = '''
                                query {
                                  reserves(where:{symbol:"%s"})
                                  { 
                                    id
                                    liquidityRate
                                    symbol
                                    name
                                    variableBorrowRate
                                    stableBorrowRate
                                    utilizationRate
                                    totalLiquidity
                                    availableLiquidity
                                    decimals
                                  }
                                }
                                '''
        V2_sample_transport             = RequestsHTTPTransport(
           url='https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
           verify=True,
           retries=5)
        V1_sample_transport             = RequestsHTTPTransport(
                   url='https://api.thegraph.com/subgraphs/name/aave/protocol-multy-raw',
                   verify=True,
                   retries=5)     
        MATIC_sample_transport          = RequestsHTTPTransport(
                   url='https://api.thegraph.com/subgraphs/name/aave/aave-v2-matic',
                   verify=True,
                   retries=5)    
        Avalanche_sample_transport      = RequestsHTTPTransport(
                   url='https://api.thegraph.com/subgraphs/name/aave/protocol-v2-avalanche',
                   verify=True,
                   retries=5,)
        self.V2_client                  = Client(transport=V2_sample_transport)
        self.V1_client                  = Client(transport=V1_sample_transport)
        self.MATIC_client               = Client(transport=MATIC_sample_transport)
        self.Avalanche_client           = Client(transport=Avalanche_sample_transport)
        self.RAY                        = pow(10, 27)
        self.AMM_Tokens                 = []
        
        
        req_date = datetime(2021,12,21).date().strftime("%m-%d-%Y") 
        aave_v2_price_response = requests.get('https://aave-api-v2.aave.com/data/liquidity/v2?poolId=0xb53c1a33016b2dc2ff3653530bff1848a515c8c5&date='+str(req_date))      
        aave_v2_price_dict_backup = {}
        ERROR_SYMBOLS             = []
        for res in aave_v2_price_response.json():
            try:
                # print(res['symbol'])
                aave_v2_price_dict_backup[res['symbol']] = float(res['referenceItem']['priceInUsd'])
            except Exception as e:
                ERROR_SYMBOLS.append(res['symbol'])

                
            
        for symbol in ERROR_SYMBOLS:
            if symbol == 'STETH':
                aave_v2_price_dict_backup[res['symbol']] = aave_v2_price_dict_backup['WETH']
            else:
                aave_v2_price_dict_backup[res['symbol']] = aave_v2_price_dict_backup['USDC']
                raise Exception("New Token Found")
        
        req_date = (datetime.now() - timedelta(1)).date().strftime("%m-%d-%Y") 
        aave_v2_price_response = requests.get('https://aave-api-v2.aave.com/data/liquidity/v2?poolId=0xb53c1a33016b2dc2ff3653530bff1848a515c8c5&date='+str(req_date))      
        self.aave_v2_price_dict = {}
        if len(aave_v2_price_response.json()) == 0:
            self.aave_v2_price_dict  = aave_v2_price_dict_backup
        else:
            for res in aave_v2_price_response.json():
                self.aave_v2_price_dict[res['symbol']] = float(res['referenceItem']['priceInUsd'])
            self.aave_v2_price_dict_backup = self.aave_v2_price_dict
        
        
    def V2_update(self):
        response   = self.V2_client.execute(gql(self.graph_token_query))
        V2_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol']:
                V2_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
                
        for TOKEN_KEY in V2_tokens:  
            try:
                ITER_ = {}
                ITER  = {}
                response                         = self.V2_client.execute(gql(self.graph_base_query % TOKEN_KEY))
                ITER['liquidityRate']            = float(response['reserves'][0]['liquidityRate'])
                ITER['variableBorrowRate']       = float(response['reserves'][0]['variableBorrowRate'])
                ITER['stableBorrowRate']         = float(response['reserves'][0]['stableBorrowRate'])
                ITER['utilizationRate']          = float(response['reserves'][0]['utilizationRate'])
                ITER['DECIMAL']                  = float(response['reserves'][0]['decimals'])
                ITER['availableLiquidity']       = float(response['reserves'][0]['availableLiquidity'])  / pow( 10 , ITER['DECIMAL'])
                ITER['MARKET']                   = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
                ITER['totalBorrow']              = ITER['MARKET']  * ITER['utilizationRate'] 
                ITER['percentDepositAPY']        = round(100 * ITER['liquidityRate']/self.RAY,      5)
                ITER['percentVariableBorrowAPY'] = round(100 * ITER['variableBorrowRate']/self.RAY, 5)
                ITER['percentStableBorrowAPY']   = round(100 * ITER['stableBorrowRate']/self.RAY,  10)
                
                self.V2_Liabilities              =  ITER['MARKET']      * self.aave_v2_price_dict[TOKEN_KEY]
                self.V2_Assets                   =  ITER['totalBorrow'] * self.aave_v2_price_dict[TOKEN_KEY]
                self.V2_LIABILITIES_SUM          += ITER['MARKET']      * self.aave_v2_price_dict[TOKEN_KEY]
                self.V2_Assets_SUM               += ITER['totalBorrow'] * self.aave_v2_price_dict[TOKEN_KEY]
                ITER_[TOKEN_KEY]                 = ITER
                self.OUTPUT['response']['data']['AAVEV2'].append(ITER_)                 
            except Exception as e:
                print("ERROR ", e)
                self.errors.append(TOKEN_KEY + " TOKEN NOT EXISTS on V2 subgraph")
        
    def V1_update(self):
        
        response   = self.V1_client.execute(gql(self.graph_token_query))
        V1_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol'] and 'Uni' not in reserve['symbol']:
                V1_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
        
        
        for TOKEN_KEY in V1_tokens:           
            try:
                ITER_ = {}
                ITER  = {}
                response                          = self.V1_client.execute(gql(self.graph_base_query % TOKEN_KEY))
                ITER['liquidityRate']             = float(response['reserves'][0]['liquidityRate'])
                ITER['variableBorrowRate']        = float(response['reserves'][0]['variableBorrowRate'])
                ITER['stableBorrowRate']          = float(response['reserves'][0]['stableBorrowRate'])
                ITER['utilizationRate']           = float(response['reserves'][0]['utilizationRate'])
                ITER['DECIMAL']                   = float(response['reserves'][0]['decimals'])
                ITER['availableLiquidity']        = float(response['reserves'][0]['availableLiquidity'])  / pow( 10 , ITER['DECIMAL'])
                ITER['MARKET']                    = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
                ITER['totalBorrow']               = ITER['MARKET']  * ITER['utilizationRate'] 
                ITER['percentDepositAPY']         = round(100 * ITER['liquidityRate']/self.RAY,      5)
                ITER['percentVariableBorrowAPY']  = round(100 * ITER['variableBorrowRate']/self.RAY, 5)
                ITER['percentStableBorrowAPY']    = round(100 * ITER['stableBorrowRate']/self.RAY,  10)
                
                if TOKEN_KEY in ['REP','LEND','ETH']:
                    if TOKEN_KEY == 'ETH':
                        TOKEN_KEY = 'WETH'
                    else:
                        TOKEN_KEY = 'USDC'

                self.V1_Liabilities               =  ITER['MARKET']      * self.aave_v2_price_dict[TOKEN_KEY]
                self.V1_Assets                    =  ITER['totalBorrow'] * self.aave_v2_price_dict[TOKEN_KEY]
                self.V1_LIABILITIES_SUM           += ITER['MARKET']      * self.aave_v2_price_dict[TOKEN_KEY]
                self.V1_Assets_SUM                += ITER['totalBorrow'] * self.aave_v2_price_dict[TOKEN_KEY]
                
                ITER_[TOKEN_KEY]                  = ITER
                self.OUTPUT['response']['data']['V1'].append(ITER_)
            except Exception as e:
                print("ERROR ", e)
                self.errors.append(TOKEN_KEY + " TOKEN NOT EXISTS on V1 subgraph")

    
    def MATIC_update(self):
        
        response     = self.MATIC_client.execute(gql(self.graph_token_query))
        MATIC_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol']:
                MATIC_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
                
        for TOKEN_KEY in MATIC_tokens:   
            try:
                ITER_ = {}
                ITER  = {}
                response                         = self.MATIC_client.execute(gql(self.graph_base_query % TOKEN_KEY))
                ITER['liquidityRate']            = float(response['reserves'][0]['liquidityRate'])
                ITER['variableBorrowRate']       = float(response['reserves'][0]['variableBorrowRate'])
                ITER['stableBorrowRate']         = float(response['reserves'][0]['stableBorrowRate'])
                ITER['utilizationRate']          = float(response['reserves'][0]['utilizationRate'])
                ITER['DECIMAL']                  = float(response['reserves'][0]['decimals'])
                ITER['availableLiquidity']       = float(response['reserves'][0]['availableLiquidity'])  / pow( 10 , ITER['DECIMAL'])
                ITER['MARKET']                   = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
                ITER['totalBorrow']              = ITER['MARKET']  * ITER['utilizationRate'] 
                ITER['percentDepositAPY']        = round(100 * ITER['liquidityRate']/self.RAY,      5)
                ITER['percentVariableBorrowAPY'] = round(100 * ITER['variableBorrowRate']/self.RAY, 5)
                ITER['percentStableBorrowAPY']   = round(100 * ITER['stableBorrowRate']/self.RAY,  10)
                if TOKEN_KEY == 'WMATIC':
                    self.MATIC_Liabilities           = ITER['MARKET']      * 2
                    self.MATIC_Assets                = ITER['totalBorrow'] * 2 
                    self.MATIC_LIABILITIES_SUM       += ITER['MARKET']     * 2
                    self.MATIC_Assets_SUM            += ITER['totalBorrow']* 2
                
                else:
                    self.MATIC_Liabilities           = ITER['MARKET']      * self.aave_v2_price_dict[TOKEN_KEY]
                    self.MATIC_Assets                = ITER['totalBorrow'] * self.aave_v2_price_dict[TOKEN_KEY]
                    self.MATIC_LIABILITIES_SUM       += ITER['MARKET']     * self.aave_v2_price_dict[TOKEN_KEY]
                    self.MATIC_Assets_SUM            += ITER['totalBorrow']* self.aave_v2_price_dict[TOKEN_KEY]
                
                ITER_[TOKEN_KEY]                  = ITER
                self.OUTPUT['response']['data']['MATIC'].append(ITER_)
            except Exception as e:
                print("ERROR ", e)
                self.errors.append(TOKEN_KEY + " TOKEN NOT EXISTS on V1 subgraph")
        
        
    def AVALANCHE_update(self):
        
        response         = self.Avalanche_client.execute(gql(self.graph_token_query))
        AVALANCHE_tokens = []
        for reserve in response['reserves']:
            if 'Amm' not in reserve['symbol']:
                AVALANCHE_tokens.append(reserve['symbol'])
            else:
                self.AMM_Tokens.append(reserve['symbol'])
        
        for TOKEN_KEY in AVALANCHE_tokens:           
            try:
                ITER_ = {}
                ITER  = {}
                response                         = self.Avalanche_client.execute(gql(self.graph_base_query % TOKEN_KEY))
                ITER['liquidityRate']            = float(response['reserves'][0]['liquidityRate'])
                ITER['variableBorrowRate']       = float(response['reserves'][0]['variableBorrowRate'])
                ITER['stableBorrowRate']         = float(response['reserves'][0]['stableBorrowRate'])
                ITER['utilizationRate']          = float(response['reserves'][0]['utilizationRate'])
                ITER['DECIMAL']                  = float(response['reserves'][0]['decimals'])
                ITER['availableLiquidity']       = float(response['reserves'][0]['availableLiquidity'])  / pow( 10 , ITER['DECIMAL'])
                ITER['MARKET']                   = (ITER['availableLiquidity'] / (1 - ITER['utilizationRate']))
                ITER['totalBorrow']              = ITER['MARKET']  * ITER['utilizationRate'] 
                ITER['percentDepositAPY']        = round(100 * ITER['liquidityRate']/self.RAY,      5)
                ITER['percentVariableBorrowAPY'] = round(100 * ITER['variableBorrowRate']/self.RAY, 5)
                ITER['percentStableBorrowAPY']   = round(100 * ITER['stableBorrowRate']/self.RAY,  10)
                
                TOKEN_KEY = TOKEN_KEY.split('.')[0]
            
                if TOKEN_KEY == 'WAVAX':
                    self.Avalanche_Liabilities       = ITER['MARKET']       * 120
                    self.Avalanche_Assets            = ITER['totalBorrow']  * 120
                    self.Avalanche_LIABILITIES_SUM   += ITER['MARKET']      * 120
                    self.Avalanche_Assets_SUM        += ITER['totalBorrow'] * 120

                else:
                    self.Avalanche_Liabilities       = ITER['MARKET']       * self.aave_v2_price_dict[TOKEN_KEY]
                    self.Avalanche_Assets            = ITER['totalBorrow']  * self.aave_v2_price_dict[TOKEN_KEY]
                    self.Avalanche_LIABILITIES_SUM   += ITER['MARKET']      * self.aave_v2_price_dict[TOKEN_KEY]
                    self.Avalanche_Assets_SUM        += ITER['totalBorrow'] * self.aave_v2_price_dict[TOKEN_KEY]
                    
                    
                ITER_[TOKEN_KEY]                 = ITER
                self.OUTPUT['response']['data']['AVALANCHE'].append(ITER_)
            except Exception as e:
                print("ERROR ", e)
                self.errors.append(TOKEN_KEY + str(e)+ " TOKEN NOT EXISTS on AVALANCHE subgraph")
        
    
    def calculate(self):
        
        self.V2_update()
        self.V1_update()
        self.MATIC_update()
        self.AVALANCHE_update()
        
        if len(self.errors)>0:
            self.OUTPUT['status'] = 300
        
    
    def get_total_Assets(self):
        return self.V2_Assets_SUM + self.V1_Assets_SUM + self.MATIC_Assets_SUM + self.Avalanche_Assets_SUM

    def get_total_Liabilities(self):
        print(self.V2_LIABILITIES_SUM , self.V1_LIABILITIES_SUM , self.MATIC_LIABILITIES_SUM , self.Avalanche_LIABILITIES_SUM)
        LIAB = self.V2_LIABILITIES_SUM + self.V1_LIABILITIES_SUM + self.MATIC_LIABILITIES_SUM + self.Avalanche_LIABILITIES_SUM
        return LIAB
    
    def set_get_V2_RATIO(self):
        print("DEBUG ", self.V2_LIABILITIES_SUM , self.get_total_Liabilities())
        self.OUTPUT['response']['result']['AAVEV2']['data']['v2_ratio'] = self.V2_LIABILITIES_SUM / self.get_total_Liabilities()
        return self.OUTPUT['response']['result']['AAVEV2']['data']['v2_ratio']
    
    def set_AAVE_MARKET_CAP(self):
        response = requests.get('https://api.coingecko.com/api/v3/coins/aave/market_chart?vs_currency=usd&days=1').json()
        self.AAVE_MARKET_CAP = response['market_caps'][-1][1]
        self.OUTPUT['response']['result']['AAVEV2']['data']['market_cap'] = self.AAVE_MARKET_CAP
    
    def get_LCR(self):
        print("Fetching LiabilitiesCAP ")
        self.set_AAVE_MARKET_CAP()
        # print("MARKET CAP : ",self.AAVE_MARKET_CAP)
        self.OUTPUT['response']['result']['AAVEV2']['data']['total_assets'] = self.V2_Assets_SUM# self.get_total_Assets()
        self.OUTPUT['response']['result']['AAVEV2']['data']['total_liabilities'] = self.V2_LIABILITIES_SUM# self.get_total_Liabilities()
        
        #v2_market_cap * (1-30%) / (v2_Assets * 0.2+ v2_assets * 0.1)
        self.OUTPUT['response']['result']['AAVEV2']['data']['lcr'] = (self.AAVE_MARKET_CAP * self.set_get_V2_RATIO() * 0.7 ) / \
                                                    ( self.V2_LIABILITIES_SUM * 0.2 + self.V2_Assets_SUM * 0.1 )      

def lambda_handler(event, context):
    print("Handler called")
    response = {}
    transactionResponse = {}
    try:
        marketExtractor = TotalMarketExtractor()
        marketExtractor.calculate()
        marketExtractor.get_LCR()
        transactionResponse  = marketExtractor.OUTPUT 
        

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
#     input_json = json.dumps(input_data)
#     result = lambda_handler(input_json,"Context")
    # print(result)
