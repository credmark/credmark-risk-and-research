import json
import os
import logging
import sys
import math
import pandas as pd
from datetime import timedelta
from datetime import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pymysql
from sqlalchemy import create_engine

import rds_config


# DB Connection
#rds settings
rds_host  = "aws-rds-test.c2mioktk9se1.ap-south-1.rds.amazonaws.com"
name      = rds_config.db_username
password  = rds_config.db_password
db_name   = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    db_data = 'mysql+pymysql://' + str(name) + ':' + str(password) + '@' + rds_host + ':3306/' \
       + str(db_name) + '?charset=utf8mb4'
    engine = create_engine(db_data)
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=60)
    # conn = pymysql.connect(host=rds_host, user=name, passwd=password, connect_timeout=20)
    # print("DB Connected")

except pymysql.MySQLError as e:
    print("DB Not connected")
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")



url='https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-prod'

'''GraphQl'''
def query_univ3(url,query_a,params):

    sample_transport=RequestsHTTPTransport(
       url=url,
       verify=True,
       retries=5,)
    client = Client(transport=sample_transport)
    query = gql(query_a)
    response = client.execute(query,variable_values=params)
    
    return response
'''Query all the swaps in a loop for a given date-range'''
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
'''Query liquidity for poolhourdata in a loop for a given date range'''
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

def get_data(start_date,end_date):
    df = pd.read_csv('data/'+ POOL_ID +'.csv')
    return df[(pd.to_datetime(df['datetime'])> start_date) & (pd.to_datetime(df['datetime'])< end_date)]

def Prepare_swap_data(POOL_ID,start_date,end_date ):

    
    swaps          = extract_swaps(start_date, end_date, POOL_ID)
    liquidity, ori = extract_liquitidity(start_date, end_date, POOL_ID)
    ticks          = extract_ticks( POOL_ID)

    liquidity_data = liquidity[pd.to_numeric(liquidity['periodStartUnix']) > pd.to_numeric(swaps['timestamp']).min()]
    liquidity_data = liquidity[pd.to_numeric(liquidity['periodStartUnix']) < pd.to_numeric(swaps['timestamp']).max()]

    marks          = liquidity_data['periodStartUnix'].values
    liquidity      = liquidity_data['liquidity'].values
    token0Price    = liquidity_data['token0Price'].values
    token1Price    = liquidity_data['token1Price'].values
    sqrtPrice      = liquidity_data['sqrtPrice'].values
    open_          = liquidity_data['open'].values
    close_         = liquidity_data['close'].values
    swaps_data     = swaps.copy()

    for i , _ in enumerate(marks[:-1]):
        cond       = ( pd.to_numeric(swaps_data['timestamp'])<=marks[i+1]) & ( pd.to_numeric(swaps_data['timestamp'])>=marks[i])
        swaps_data.loc[cond, 'liquidity']   = liquidity[i]
        swaps_data.loc[cond, 'token0Price'] = token0Price[i]
        swaps_data.loc[cond, 'token1Price'] = token1Price[i]
        swaps_data.loc[cond, 'sqrtPrice']   = sqrtPrice[i]
        swaps_data.loc[cond, 'open']        = open_[i]
        swaps_data.loc[cond, 'close']       = close_[i]

    swaps_data['datetime']                   = swaps_data['timestamp'].apply(lambda x: datetime.fromtimestamp(float(x)))
    swaps_data.reset_index(inplace=True)
    swaps_data = swaps_data.dropna()
    del swaps_data['index']
    swaps_data=swaps_data.drop_duplicates().reset_index(drop=True)
    swaps_data=swaps_data[(pd.to_datetime(swaps_data['datetime'])>= start_date) & (pd.to_datetime(swaps_data['datetime'])<= end_date)]
    swaps_data  =  swaps_data.reset_index(drop = True)
    return swaps_data


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

  
def CheckData(start_date, end_date, pool_id):
    slots = []
    STEP = 15

    # To remove the leading 0, as stored for table names in the database
    pool_id = pool_id[1:]
    tableExists = checkTable(pool_id)

    # if os.path.exists('data/'+ str(pool_id)+'.csv'):
    if tableExists:
        df = pd.read_sql("SELECT * FROM {0}".format(str(pool_id)), con=conn)

        available_start_date  = pd.to_datetime(df['datetime'].iloc[0])
        available_end_date    = pd.to_datetime(df['datetime'].iloc[-1]) # last element 

        # print(" available_start_date  ", available_start_date, '\n', 
        #       "start_date            ", start_date, '\n',
        #       "available_end_date    ", available_end_date,'\n',
        #       "end_date              ", end_date)
            

        if ( available_start_date - start_date).days < 1:
            # print("Staring data already available")
            pass
        else:
            
            if (start_date - available_start_date).days < 15 and ((available_start_date - start_date).days // 15 ) == 0 :
                # print(" DEBUG 6")
                slots.append((available_start_date  - timedelta(days = STEP )  , available_start_date))
            
            else:
                
                for iteration in range((available_start_date - start_date).days // 15 + 1):
                    # print(" DEBUG 6")
                    slots.append((available_start_date - timedelta(days = STEP * (iteration + 1)) , available_start_date - timedelta(days = STEP * (iteration))))
                    

        if (end_date - available_end_date).days < 1:
            print("Ending data already available")
            pass
        else:
            # print("Missing data for ", (end_date - available_end_date).days)
            
            if (end_date - available_end_date).days < 15 and ((end_date - available_end_date).days // 15 + 1) ==0 :
                # print(" DEBUG 1")
                slots.append((available_end_date ,available_end_date - timedelta(days = STEP)  ))

            else:

                for iteration in range((end_date - available_end_date).days // 15 +1):
                    # print(" DEBUG 2")
                    slots.append( (  available_end_date + timedelta(days = STEP * (iteration)) , available_end_date + timedelta(days = STEP * (iteration + 1))))
                
        
    
    else:
        # print("POOL DATA DOES NOT EXISTS ")
        if (end_date - start_date).days > 15:
            for iteration in range((end_date - start_date).days // 15 + 1):
                # print(" DEBUG 3")
                slots.append((start_date + timedelta(days = STEP * (iteration )) , start_date + timedelta(days = STEP * (iteration + 1))))
                #return Prepare_swap_data(pool_id, start_date - datetime.timedelta(days = STEP * (iteration ), start_date + datetime.timedelta(days = STEP * (iteration + 1)))
        else:
            # print(" DEBUG 4")
            slots.append((start_date , end_date))

    # for i in  slots:
    #     print("SLOTS ", i)

    return slots

def tickcalc(input_data):
    return math.floor(np.log(math.sqrt(input_data)) / np.log(math.sqrt(1.0001)))

def pricetickcalc(input_data):
    return np.log(math.sqrt(input_data))/np.log(math.sqrt(1.0001))

def priceFromTick(input_data):
    return pow(1.0001, input_data)

def DateTimeConverter(input_date):
    return datetime.utcfromtimestamp(input_date).strftime('%Y-%m-%d %H:%M:%S')

def lambda_handler(event, context):
    print("Handler called")
    response = {}
    transactionResponse = {}
    try:
        # event       = json.loads(event)
        payload     = event["body"]
        poolAddress = payload["address"]
        start_date  = datetime.strptime(payload['start_date'], "%Y-%m-%d %H:%M:%S")
        end_date    = datetime.strptime(payload['end_date'], "%Y-%m-%d %H:%M:%S")
        # print((end_date-start_date).seconds/3600)
        # exit()
        # cur = conn.cursor()
        # TABLE_QUERY="DROP TABLE {0}".format(str(poolAddress)[1:])
        # cur.execute(TABLE_QUERY)
        # print("As")
        # exit()
        z = (end_date.timestamp() - start_date.timestamp())
        print("Time Test 3600")
        print((end_date-start_date).seconds/3600)
        print(end_date)
        print(start_date)
        print(z)
        z /= 3600
        print(z)
        print("Done")
        # if end_date>start_date and ((end_date-start_date).seconds/3600)>1:
        if end_date>start_date and z>1:
            slot_swap_slots = Prepare_swap_data(poolAddress, start_date, end_date)

            cur = conn.cursor()

            if not checkTable(str(poolAddress)[1:]):
                print("--------TABLE CREATED------")
                TABLE_QUERY = """
                    CREATE TABLE {0} (
                    amount0 DECIMAL(30, 15) NOT NULL,
                    amount1 DECIMAL(30, 15) NOT NULL,
                    amountUSD DECIMAL(30,15) NOT NULL,
                    id tinytext NOT NULL,
                    sqrtPriceX96 tinytext not null,
                    tick int not null,
                    timestamp tinytext not null,
                    timestamp_1 tinytext not null,
                    liquidity tinytext not null,
                    token0Price decimal(40, 25) not null,
                    token1Price decimal(40,25) not null,
                    sqrtPrice tinytext not null,
                    open decimal(40, 25) not null,
                    close decimal(40, 25) not null,
                    datetime datetime not null
                )""".format(str(poolAddress)[1:])
                cur.execute(TABLE_QUERY)

            # print(slot_swap_slots.duplicated().sum())
            slot_swap_slots.to_sql(str(poolAddress)[1:], engine, if_exists='append', index=False)
            df = pd.read_sql("SELECT * FROM {0}".format(str(poolAddress)[1:]), con=conn)
            df.to_csv("exp_dataframe1.csv",index=False)
        else:
            print("NO CHNAGE IN DATABASE")
        
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
    response_JSON = json.dumps(response, default = str)

    # return response
    return response_JSON


# if __name__ == "__main__":

#     input_data = {
#           "body": {
#             "query": "poolDayDatas",
#             "address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
#             "graph": "v3_testing",
#             "feeAmount":3000,
#             "start_date": "2021-08-15 00:00:00" ,
#             "end_date":  "2021-08-29 00:00:00",
#           }
#         }

#     input_json = json.dumps(input_data)
#     result = lambda_handler(input_data,"Context")
#     print("Lambda Result")
#     print(result)
