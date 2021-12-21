import json
from math import exp
import os
import math
import logging
import sys

import pandas as pd
from datetime import timedelta
from datetime import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pymysql
pd.options.mode.chained_assignment = None

import rds_config


# DB Connection
#rds settings
rds_host  = "aws-rds-test.c2mioktk9se1.ap-south-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=20)
    # conn = pymysql.connect(host=rds_host, user=name, passwd=password, connect_timeout=20)
    # print("DB Connected")
except pymysql.MySQLError as e:
    print("DB Not connected")
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")




url='https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-prod'





#GraphQl
def query_univ3(url,query_a,params):

    sample_transport=RequestsHTTPTransport(
       url=url,
       verify=True,
       retries=5,)
    client = Client(transport=sample_transport)
    query = gql(query_a)
    response = client.execute(query,variable_values=params)
    
    return response
#Query all the swaps in a loop for a given date-range
def extract_swaps(begin,end,pool_id):
    query='''query manyswaps($last_ts: String, $pool_id:String)
        {
            swaps(first:1000,orderBy:timestamp,orderDirection:asc 
            where:{timestamp_gte: $last_ts pool: $pool_id }) 
            {id,timestamp,token0, token1, amount0,amount1,amountUSD,tick,sqrtPriceX96}
            }
            '''
    ts_inicio = int((begin - datetime(1970, 1, 1)).total_seconds())
    ts_fin = int((end - datetime(1970, 1, 1)).total_seconds())
    
    swaps=pd.DataFrame()  
    last_swapId=""
    
    while int(ts_inicio)<ts_fin-10:
    
        params ={"last_ts": str(ts_inicio),"pool_id":pool_id}   
        a=query_univ3(url,query,params)
 
        swap_data=pd.json_normalize(a['swaps'])
        swap_data['timestamp_1']=[datetime.utcfromtimestamp((int(x))) for x in swap_data['timestamp']]
        swapId=swap_data['id'].iloc[-1]
        
        if swapId==last_swapId:
            break
        else:
            swaps=swaps.append((swap_data))
            ts_inicio=swap_data['timestamp'].iloc[-1]
            last_swapId=swap_data['id'].iloc[-1]

    return swaps 
#Query liquidity for poolhourdata in a loop for a given date range
def extract_liquitidity(begin,end,pool_id):
    
    query='''query liquidity_data($last_ts: Int, $pool_id:String)
        {
            poolHourDatas(first:1000,orderBy:periodStartUnix,orderDirection:asc 
            where:{periodStartUnix_gte: $last_ts pool: $pool_id }) 
            {id,periodStartUnix,liquidity,sqrtPrice,tick, token0Price, token1Price,open,close}
            }
            '''
    ts_inicio = int((begin - datetime(1970, 1, 1)).total_seconds())-60*60
    ts_fin = int((end - datetime(1970, 1, 1)).total_seconds())
    liquidity_data=pd.DataFrame()  
    last_hourId=""
    
    while int(ts_inicio)<ts_fin-10:
    
        params ={"last_ts": int(ts_inicio),"pool_id":pool_id}   
        a=query_univ3(url,query,params)
    
        #df with swap_data
        liquidity_data_row=pd.json_normalize(a['poolHourDatas'])
        liquidity_data_row['timestamp_1']=[datetime.utcfromtimestamp((int(x))) for x in liquidity_data_row['periodStartUnix']]
        hourId=liquidity_data_row['id'].iloc[-1]
        
        if hourId==last_hourId:
            break
        else:
            liquidity_data=liquidity_data.append((liquidity_data_row))
            ts_inicio=liquidity_data['periodStartUnix'].iloc[-1]
            last_hourId=liquidity_data['id'].iloc[-1]
    #timestamp refers to beginning and data to end       
    liq_copy = liquidity_data.copy()
    liquidity_data['liquidity']=liquidity_data['liquidity'].shift(1)
    liquidity_data['tick']=liquidity_data['tick'].shift(1)
    liquidity_data=liquidity_data.dropna()
    return liquidity_data     , liq_copy
def extract_ticks(pool_id):
    
    query='''query pl($pool_id:String) 
    {pools(where: { id: $pool_id } ) 
        {
        token0 {symbol decimals}
        token1 {symbol decimals}
        feeTier

        ticks(first:500) {price0,price1,tickIdx, liquidityGross,liquidityNet}
        }
        }'''
    
    params ={"pool_id":pool_id} 
    a=query_univ3(url,query,params)

    pool_data=pd.json_normalize(a['pools'])
    tick_data=pd.json_normalize(a['pools'][0]['ticks'])
    token0=pool_data['token0.symbol'].iloc[0]
    token1=pool_data['token1.symbol'].iloc[0]
    decimal0=int(pool_data['token0.decimals'].iloc[0])
    decimal1=int(pool_data['token1.decimals'].iloc[0])
    feeTier=int(pool_data['feeTier'].iloc[0])
    tick_data['t0']=token0
    tick_data['t1']=token1
    tick_data['pool_name']=token0+"-"+token1+"-"+str(feeTier)
    tick_data['tickIdx']=tick_data['tickIdx'].astype(int)
    tick_data['price1']=1.0001**tick_data['tickIdx']/10**(decimal1-decimal0)
    tick_data['price0']=1/tick_data['price1']

    
    return tick_data

def get_created(pool_id):
    
    query_slot0='''query pl($pool_id:String) 
    {pools(where: { id: $pool_id } ) 
        {
        createdAtTimestamp
        }
        }'''
    params ={"pool_id":pool_id}          
    a=query_univ3(url,query_slot0,params)
    slot0_data=pd.json_normalize(a['pools'])
     
    return slot0_data

def date_validation(start_date,end_date):
    pool_created_date = int(get_created(POOL_ID)['createdAtTimestamp'])
    print(" Pool created at ", datetime.fromtimestamp(pool_created_date))

    if end_date.timestamp() < pool_created_date:
        print("Range not valid, out of pool timetime")
        exit()

    if start_date.timestamp() < pool_created_date:
        print("Start date earlier than pool creation, changing start date to %s", str(datetime.fromtimestamp(pool_created_date)))
        start_date = datetime.fromtimestamp(int(pool_created_date))

    return start_date, end_date

def checkTable(pool_id):
    cur = conn.cursor()
    print("Pool ID Test")
    print(pool_id)
    stmt = "SHOW TABLES LIKE '{0}'".format(str(pool_id))
    cur.execute(stmt)
    result = cur.fetchone()
    if result:
        print("Table Exists")
        return True
    else:
        return False


def get_data(poolAddress,start_date,end_date):
    # df = pd.read_csv('data/'+ poolAddress +'.csv')
    # df =  df[(pd.to_datetime(df['datetime'])> start_date) & (pd.to_datetime(df['datetime'])< end_date)]
    
    # df[(pd.to_datetime(df['datetime'])> start_date) & (pd.to_datetime(df['datetime'])< end_date)]
    # df = pd.read_sql("SELECT * FROM {0}".format(str(poolAddress)[1:]), con=conn)
    # print(start_date.split(' ')[0],end_date.split(' ')[0])

    df = pd.read_sql("SELECT * FROM {0} WHERE (timestamp >= {1} AND timestamp <= {2})".format(str(poolAddress)[1:], str(start_date.timestamp()),str(end_date.timestamp())), con=conn)
    # df = pd.read_sql("SELECT * FROM {0}".format(str(poolAddress)[1:]), con=conn)

    # print("Testing Date Read")
    # print(df)
    
    try:
        del df['Unnamed: 0']
    except:
        pass
    try:
        del df['index']
    except:
        pass
    df = df.reset_index()

    # df.to_csv("del.csv")
    return df

def Fees_accrued(swaps_data, fee_tier, lower_price,upper_price, amount_invested,factor, tick_spacing, decimals0,decimals1):
    fees=swaps_data.copy()
    tick_lower,tick_upper = get_ticks(lower_price, upper_price,tick_spacing,decimals0,decimals1)
    print("---------------",tick_lower,tick_upper,"------------------------")
    current_tick=int(swaps_data[0:1]['tick'].values[0])
    r    =  amounts_relation(current_tick,tick_lower,tick_upper,decimals0,decimals1)
    cp   =  1/(1.0001**float(current_tick)/10**(decimals1-decimals0))
    amount0=amount_invested/(r*cp+1)
    amount1=amount0*r
    initial_liquidity= get_liquidity(current_tick,tick_lower,tick_upper,amount0,amount1,decimals0,decimals1)
    
    fees['liquidity_user']=initial_liquidity
    fees['fees_token0']=fees.apply(lambda x: fees_func(x['amount0'],x['liquidity_user'],x['liquidity'],fee_tier,tick_lower,tick_upper,float(x['tick']),True) ,axis=1)
    fees['fees_token1']=fees.apply(lambda x: fees_func(x['amount1'],x['liquidity_user'],x['liquidity'],fee_tier,tick_lower,tick_upper,float(x['tick']),True) ,axis=1)
    fees['fees_usd']=fees.apply(lambda x: fees_func(x['amountUSD'],x['liquidity_user'],x['liquidity'],fee_tier,tick_lower,tick_upper,float(x['tick'])) ,axis=1)
    fees['fees_usd_in_range']=fees.apply(lambda x: fees_func(x['amountUSD'],x['liquidity'],x['liquidity'],1,tick_lower,tick_upper,float(x['tick'])) ,axis=1)
    fees['collected_fees_token0']=fees['fees_token0']
    fees['collected_fees_token1']=fees['fees_token1']
    fees['collected_fees_usd']=fees['fees_usd']
    fees=fees.reset_index(drop=True)
    for i in range(1,len(fees)):
        fees['collected_fees_token0'][i]=fees['collected_fees_token0'][i-1]+fees['fees_token0'][i]
        fees['collected_fees_token1'][i]=fees['collected_fees_token1'][i-1]+fees['fees_token1'][i]
        fees['collected_fees_usd'][i]=fees['collected_fees_usd'][i-1]+fees['fees_usd'][i]
    return fees[['collected_fees_token0','collected_fees_token1','collected_fees_usd','timestamp']]
def calculate_IL(swaps_data, lower_price,upper_price, amount_invested,factor, tick_spacing, decimals0,decimals1):
    tick_lower,tick_upper = get_ticks(lower_price, upper_price,tick_spacing,decimals0,decimals1)
    current_tick = int(swaps_data[0:1]['tick'].values[0])
    r=amounts_relation(current_tick,tick_lower,tick_upper,decimals0,decimals1)
    cp= 1/(1.0001**float(current_tick)/10**(decimals1-decimals0))
    amount0=amount_invested/(r*cp+1)
    amount1=amount0*r
    initial_liquidity= get_liquidity(current_tick,tick_lower,tick_upper,amount0,amount1,decimals0,decimals1)
    test=swaps_data.copy()
    test['tick']=swaps_data['tick']
    test['liquidity']=initial_liquidity
    test['sqrt']=test['tick'].apply(lambda x:1.0001**(float(x)/2)*2**96)#(1.0001**(pd.to_numeric(test['tick'])/2))*2**96
    test['sqrtA']       = 1.0001**(tick_lower/2)*(2**96)
    test['sqrtB']       = 1.0001**(tick_upper/2)*(2**96)
    test['amounts_0']   = test.apply(lambda x: get_amounts(x['sqrt'],x['sqrtA'],x['sqrtB'],x['liquidity'],decimals0,decimals1)[0] ,axis=1)
    test['amounts_1']   = test.apply(lambda x: get_amounts(x['sqrt'],x['sqrtA'],x['sqrtB'],x['liquidity'],decimals0,decimals1)[1] ,axis=1)
    test['price_t1/t0'] = 1/(1.0001**pd.to_numeric(test['tick'])/10**(decimals1-decimals0))
    test['value_LP']    = (test['amounts_1']*test['price_t1/t0']+test['amounts_0'])*factor
    test['value_Hold']  = (amount1*test['price_t1/t0']+amount0)*factor
    test['IL_USD']      = test['value_LP']-test['value_Hold']
    test['ILvsHold']    = test['IL_USD']/test['value_Hold']
    return test[['IL_USD','ILvsHold']]
def calculate_PNL(swaps_data,fee_tier, lower_price,upper_price, amount_invested,gas_price,gwei,gewi_to_usd,factor, tick_spacing, decimals0,decimals1):
    Gas_costs_mint     = gwei  * gas_price
    Gas_costs_mint_USD = Gas_costs_mint * gewi_to_usd
    df                 = Fees_accrued(swaps_data, fee_tier, lower_price,upper_price, amount_invested - Gas_costs_mint_USD, factor, tick_spacing, decimals0=decimals0,decimals1=decimals1)
    Il_df              = calculate_IL(swaps_data, lower_price,upper_price, amount_invested,factor, tick_spacing,decimals0=decimals0,decimals1=decimals1)
    df['IL_USD']       = Il_df['IL_USD']
    df['ILvsHold']     = Il_df['ILvsHold']
    df['PNL']          = df['collected_fees_usd']-abs(df['IL_USD'])- (Gas_costs_mint_USD * factor[0])
    return df
def calculate_APR(swaps_data,fee_tier, lower_price,upper_price, amount_invested,gas_price,gwei,gewi_to_usd,factor, tick_spacing,decimals0,decimals1):
    df                 =  calculate_PNL(swaps_data, fee_tier, lower_price,upper_price, amount_invested,gas_price,gwei,gewi_to_usd,factor, tick_spacing,decimals0=decimals0,decimals1=decimals1)
    Gas_costs_mint     =  gwei  * gas_price
    Gas_costs_mint_USD =  Gas_costs_mint * gewi_to_usd 
    Initial_capital    =  (amount_invested-Gas_costs_mint_USD) * factor[0]
    start_time         =  float(df['timestamp'][0])
    df['age_of_the_position']=pd.to_numeric(df['timestamp'])-start_time
    year_time          =  3600*24*365
    df['APR']          =  (df['PNL']/(Initial_capital*(df['age_of_the_position'] / year_time)))*100
    return df

def query_top_pools(top_n,feature):
    
    query_top_pools='''query pl($top_n:Int, $feature:String) 
    {pools(first:$top_n,orderBy:$feature,orderDirection:desc  ) 
        {
        id
        createdAtTimestamp
        token0 {symbol decimals derivedETH}
        token1 {symbol decimals derivedETH}
        feeTier
        volumeUSD
        }
        }'''
    params ={"top_n":top_n,"feature":feature}          
    a=query_univ3(url,query_top_pools,params)
    top_pools_data=pd.io.json.json_normalize(a['pools'])
    top_pools_data['pool_name']=top_pools_data['token0.symbol']+"-"+top_pools_data['token1.symbol']+"-"+top_pools_data['feeTier']

    return top_pools_data


def get_ticks(price_lower,price_upper,tick_spacing,d0,d1):

    tick_lower = math.log(10**(d1-d0)/price_lower,1.0001)
    tick_lower = int(tick_lower)
    tick_upper = math.log(10**(d1-d0)/price_upper,1.0001)
    tick_upper = int(tick_upper)
    tick_lower=(tick_lower//tick_spacing)*tick_spacing
    tick_upper=(tick_upper//tick_spacing)*tick_spacing
    if tick_lower==tick_upper:
        print(tick_lower,tick_upper)
        tick_lower-=tick_spacing
        tick_upper+=tick_spacing
        print(tick_lower,tick_upper)
    return tick_upper,tick_lower
def get_amount0(sqrtA,sqrtB,liquidity,decimals):
    
    if (sqrtA > sqrtB):
          (sqrtA,sqrtB)=(sqrtB,sqrtA)
    
    amount0=((liquidity*2**96*(sqrtB-sqrtA)/sqrtB/sqrtA)/10**decimals)
    
    return amount0
def get_amount1(sqrtA,sqrtB,liquidity,decimals):
    
    if (sqrtA > sqrtB):
        (sqrtA,sqrtB)=(sqrtB,sqrtA)
    
    amount1=liquidity*(sqrtB-sqrtA)/2**96/10**decimals
    
    return amount1
def get_amounts(tick,tickA,tickB,liquidity,decimal0,decimal1):

    sqrt=tick#(1.0001**(tick/2)*(2**96))
    sqrtA=tickA#(1.0001**(tickA/2)*(2**96))
    sqrtB=tickB#(1.0001**(tickB/2)*(2**96))

    if (sqrtA > sqrtB):
        (sqrtA,sqrtB)=(sqrtB,sqrtA)

    if sqrt<=sqrtA:

        amount0=get_amount0(sqrtA,sqrtB,liquidity,decimal0)
        return amount0,0
   
    elif sqrt<sqrtB and sqrt>sqrtA:
        amount0=get_amount0(sqrt,sqrtB,liquidity,decimal0)
        amount1=get_amount1(sqrtA,sqrt,liquidity,decimal1)
        return amount0,amount1
    
    else:
        amount1=get_amount1(sqrtA,sqrtB,liquidity,decimal1)
        return 0,amount1
'''get token amounts relation'''  
def amounts_relation (tick,tickA,tickB,decimals0,decimals1):
    
    sqrt=(1.0001**tick/10**(decimals1-decimals0))**(1/2)
    sqrtA=(1.0001**tickA/10**(decimals1-decimals0))**(1/2)
    sqrtB=(1.0001**tickB/10**(decimals1-decimals0))**(1/2)
    
    if sqrt<=sqrtA:
        relation=10**-15
        print("There is 0 tokens on token0 side")
        return relation#0
    if sqrt>=sqrtB:
        relation=10**15
        print("There is 0 tokens on token1 side")
        return relation#0

    relation=(sqrt-sqrtA)/((1/sqrt)-(1/sqrtB))     
    return relation    
'''get_liquidity function'''
#Use 'get_liquidity' function to calculate liquidity as a function of amounts and price range
def get_liquidity0(sqrtA,sqrtB,amount0,decimals):
    
    if (sqrtA > sqrtB):
          (sqrtA,sqrtB)=(sqrtB,sqrtA)
    
    liquidity=amount0/((2**96*(sqrtB-sqrtA)/sqrtB/sqrtA)/10**decimals)
    return liquidity
def get_liquidity1(sqrtA,sqrtB,amount1,decimals):
    
    if (sqrtA > sqrtB):
        (sqrtA,sqrtB)=(sqrtB,sqrtA)
    
    liquidity=amount1/((sqrtB-sqrtA)/2**96/10**decimals)
    return liquidity
def get_liquidity(tick,tickA,tickB,amount0,amount1,decimal0,decimal1):
    sqrt=(1.0001**(tick/2)*(2**96))
    sqrtA=(1.0001**(tickA/2)*(2**96))
    sqrtB=(1.0001**(tickB/2)*(2**96))
    if (sqrtA > sqrtB):
        (sqrtA,sqrtB)=(sqrtB,sqrtA)

    if sqrt<=sqrtA:

        liquidity0=get_liquidity0(sqrtA,sqrtB,amount0,decimal0)
        return liquidity0
    elif sqrt<sqrtB and sqrt>sqrtA:

        liquidity0=get_liquidity0(sqrt,sqrtB,amount0,decimal0)
        liquidity1=get_liquidity1(sqrtA,sqrt,amount1,decimal1)
        liquidity_exp=get_liquidity1(sqrtA,sqrtB,amount1,decimal1)
        liquidity=liquidity0 if liquidity0<liquidity1 else liquidity1
        return liquidity#,liquidity0,liquidity1,liquidity_exp

    else:
        liquidity1=get_liquidity1(sqrtA,sqrtB,amount1,decimal1)
        return liquidity1
def fees_func(x,user_liquidity,total_liquidity,fee_tier,tick_lower,tick_upper,current_tick,flag=False):
    if current_tick>tick_upper or current_tick<tick_lower:
        return 0
    if float(x)>0 and flag:
        return 0
    return abs(float(x))*float(fee_tier)*(float(user_liquidity)/float(total_liquidity))



def lambda_handler(event, context):
    print("Handler called")
    response = {}
    transactionResponse = {}

    gas_price                   = 37.9200
    gwei                        = 500000
    d_tick_spacing              = {}
    d_tick_spacing["3000"]      = 60
    d_tick_spacing["500"]       = 10
    d_tick_spacing["10000"]     = 200
    try:
        payload     = event["body"]
        poolAddress = payload["address"]
        payload["feeAmount"] = payload["amountInvested"]
        AMOUNT   = payload["feeAmount"]
        
        risk_profiles = ['low', 'medium', 'high'] #payload['liquidityData']
        
        
        start_date  = datetime.strptime(payload['start_date'], "%d-%m-%Y")
        end_date    = datetime.strptime(payload['end_date'], "%d-%m-%Y")


        dataframe = get_data(poolAddress, start_date,end_date )
        dataframe=dataframe.drop_duplicates().reset_index(drop=True)
        # dataframe  = pd.read_csv('temp.csv')
        # del dataframe['Unnamed: 0']



        POOLS       = query_top_pools(150, 'totalValueLockedUSD')
        pool_df     = POOLS[POOLS['id'] == poolAddress]
        
        if len(pool_df)==0:
            response["statusCode"] = 200
            transactionResponse["error"] = True
            transactionResponse["message"] = "Pool Not exists"
            response["headers"] = {}
            response["headers"]["Content-Type"] = "application/json"
            response["body"] = transactionResponse
            response_JSON = response
            return response 

        FEE_TIER                    = int(pool_df['feeTier'])/ 1000000

        tick_spacing=d_tick_spacing[pool_df['feeTier'][0]]

        # print(dataframe.shape)

        factor       = abs(dataframe['amountUSD']/ dataframe['amount0'])
        # print("FACTOR", factor)
        AMOUNT       = 100000*(1/factor[0])
        gewi_to_usd  = 2.0082436708860756e-06*(1/factor[0])




        # print("Data Received", dataframe.shape)

        for risk in risk_profiles:
            transactionResponse[risk] = {}
            # print(risk, payload['liquidityData'][risk]['positionPriceUpper'], payload['liquidityData'][risk]['positionPriceLower'])

            try:
            
                LOWER_PRICE = 1 / float(payload['liquidityData'][risk]['positionPriceUpper'][0])
                UPPER_PRICE = 1 / float(payload['liquidityData'][risk]['positionPriceLower'][0])
                
                
                # print("Checking Fees",  LOWER_PRICE, UPPER_PRICE)
                
            

                RES = calculate_APR( dataframe, FEE_TIER , LOWER_PRICE,UPPER_PRICE, AMOUNT ,gas_price,gwei,gewi_to_usd,factor, tick_spacing,decimals0=int(pool_df['token0.decimals'][0]),decimals1=int(pool_df['token1.decimals'][0]))             
                # print("DEBUG ", RES)
                transactionResponse[risk]['APR']  = RES['APR'].iloc[-1]
                transactionResponse[risk]['PNL']  = RES['PNL'].iloc[-1]
                transactionResponse[risk]['IL']   = RES['IL_USD'].iloc[-1]
                transactionResponse[risk]['Fees'] = RES['collected_fees_usd'].iloc[-1]
            except Exception as e:
                # print("ERROR ", e)
                transactionResponse[risk]['APR']  = []
                transactionResponse[risk]['PNL']  = []
                transactionResponse[risk]['IL']   = []
                transactionResponse[risk]['Fees'] = []
        # return transactionResponse
        
    except Exception as e:

        s = str(e)
        response["statusCode"] = 400
        transactionResponse["error"] = True
        transactionResponse["message"] = s
        # transactionResponse["file"] = ""
        # transactionResponse["bucket"] = OUTPUT_BUCKET
        response["headers"] = {}
        response["headers"]["Content-Type"] = "application/json"
        # transactionResponse["score"] = "{:.5f}".format(0.00000)

        response["body"] = transactionResponse
        response_JSON = response
        
        # Revert the Response
        return response_JSON

    response["statusCode"] = 200
    transactionResponse["error"] = False
    transactionResponse["message"] = "Error free execution"
    # transactionResponse["file"] = output_path
    # transactionResponse["bucket"] = OUTPUT_BUCKET
    response["headers"] = {}
    response["headers"]["Content-Type"] = "application/json"
    # transactionResponse["score"] = "{:.5f}".format(0.00000)

    response["body"] = transactionResponse

    # Converting python dictionary to JSON Object
    response_JSON = response

    return response





# if __name__ == "__main__":

#     input_data = {
#           "body": {
#             "query": "poolDayDatas",
#             "address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
#             "graph": "v3_testing",
#             "feeAmount":5000,
#             "start_date": "10-05-2021" ,
#             "end_date":  "10-06-2021",
#               "positionPriceLower": "0.00036047863253077576",
#               "positionPriceUpper":"0.0005684188258925406",
#           }
#         }

    
#     input_data = {
#             "body": {
#                 "query": "poolDayDatas",
#                 "amountInvestedUSD": 10000,
#                 "address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
#                 "graph": "v3_testing",
#                 "feeAmount":3000,
#                 "start_date": "16-05-2021",
#                 "end_date":  "14-06-2021",
#                 "liquidityData": {
#             "low": {
#                 "positionPriceLower": [
#                     "0.0001763674958477899"
#                 ],
#                 "positionPriceUpper": [
#                     "0.00037309869308632974"
#                 ]
#             },
#             "medium": {
#                 "positionPriceLower": [
#                     "0.0002091560287208799"
#                 ],
#                 "positionPriceUpper": [
#                     "0.00034031016021323973"
#                 ]
#             },
#             "high": {
#                 "positionPriceLower": [
#                     "0.00024194456159396985"
#                 ],
#                 "positionPriceUpper": [
#                     "0.00030752162734014977"
#                 ]
#             },
#             "day": [
#                 "null"
#             ],
#             "date": [
#                 "16-05-2021"
#             ],
#             "hold": True
#         }
#             } 
#     }


#     input_json = json.dumps(input_data)
#     result = lambda_handler(input_data,"Context")
#     print("Lambda Result")
#     print(result)
    
    
