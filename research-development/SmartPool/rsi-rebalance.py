# Import packages
import json
from datetime import datetime
import math

import pandas as pd
import numpy as np
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


# Getting the credentials for graph
# Link for testing subgraph: https://thegraph.com/explorer/subgraph/ianlapham/uniswap-v3-testing
GRAPH_URL_TESTING = "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-testing"


GRAPH_API_DICT = {
    "poolDayDatas" : [
    ["(where: {pool: \"", "\" }){ "],
     ["date", 
     "sqrtPrice", 
     "token0Price", 
     "token1Price", 
     "tick", 
     "tvlUSD", 
     "volumeToken0", 
     "volumeToken1", 
     "volumeUSD", 
     "txCount", 
     "}"]

     ]
     }

feeMapping = {
    "500":10,
    "3000":60,
    "10000":200
}


def tickcalc(input_data):
    return math.floor(np.log(math.sqrt(input_data)) / np.log(math.sqrt(1.0001)))

def pricetickcalc(input_data):
    return np.log(math.sqrt(input_data))/np.log(math.sqrt(1.0001))

def priceFromTick(input_data):
    return pow(1.0001, input_data)

def DateTimeConverter(input_date):
    return datetime.utcfromtimestamp(input_date).strftime('%Y-%m-%d %H:%M:%S')


def lambda_handler(event, context):

    # print("handler called")
    # Response JSON
    response = {}
    transactionResponse = {}

    try:
        # Getting the payload
        # event = json.loads(event)
        payload = event["body"]
        # print(type(payload))

        queryType = payload["query"]
        poolAddress = payload["address"]
        graphMode = payload["graph"]
        feeAmount = payload["feeAmount"]
        tickSpacing = feeMapping[str(feeAmount)]

        positionPriceLower = payload["token1priceLower"]
        positionPriceUpper = payload["token1priceUpper"]

        # Getting position data, this has to be made sure to be coverted to the appropriate token ratio
        # positionTickLower = payload["tickLower"]
        # positionTickUpper = payload["tickUpper"]
        # positionPriceLower = payload["token1priceLower"]
        # positionPriceUpper = payload["token1priceUpper"]




        # Other mode comparisons can be added here later
        if (graphMode == "v3_testing"):
            GRAPH_URL = GRAPH_URL_TESTING


        # print("Got the test data out")


    except Exception as e:

        # If any error faced in parsing the above keys
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

        # Converting python dictionary to JSON Object
        response_JSON = response
        
        # Revert the Response
        return response_JSON


    try:
        # Creating Query architecture

        query_transport=RequestsHTTPTransport(
        url=GRAPH_URL,
        verify=True,
        retries=5,
        )

        client = Client(
            transport=query_transport
        )


        # Creating the query string
        query_str_list = [
        "query {", 
        str(queryType)+ str(GRAPH_API_DICT[queryType][0][0])+str(poolAddress)+str(GRAPH_API_DICT[queryType][0][1])
        ]

        query_str_list_1 = [str(x) for x in GRAPH_API_DICT[queryType][1]]
        query_str_list.extend(query_str_list_1)
        query_str_list.extend(["}"])

        query_str = "\n".join(query_str_list)
        # print("Checking query string")
        # print(type(query_str))
        # print(query_str)

        # Creating the actual query
        query = gql(query_str)

        queryResponse = client.execute(query)

        print("Query executed")
        print("Converting to pandas now")

        # Creating dataframe for the respose
        responseDf = pd.DataFrame(queryResponse[queryType])

        if (responseDf.columns[0] == "date"):
            responseDf['date'] = responseDf['date'].apply(DateTimeConverter)

        transactionResponse["graphData"] = {
            "date": list(responseDf["date"]),
            "sqrtPrice":list(responseDf["sqrtPrice"].astype(str)),
            "tick": list(responseDf["tick"].astype(str)),
            "price":list(pow(1.0001, responseDf["tick"].astype(float))),
            "token0Price": list(pd.to_numeric(responseDf["token0Price"])),
            "token1Price": list(pd.to_numeric(responseDf["token1Price"]))
        }


        idx_to_delete = []
        # Check for NaN values in the transactionResponse graphData for price
        for i in range(len(transactionResponse["graphData"]["price"])):
            # if (transactionResponse["graphData"]["price"][i]!=transactionResponse["graphData"]["price"][i]):
            if (transactionResponse["graphData"]["price"][i] is None): 
                    idx_to_delete.append(i)
        if idx_to_delete:
            for key in transactionResponse["graphData"].keys():
                updated_values = []
                for i in range(len(transactionResponse["graphData"][key])):
                    if(i not in idx_to_delete):
                        updated_values.append(transactionResponse["graphData"][key][i])
                transactionResponse["graphData"][key] = updated_values

        # print("Query data inserted into the graph")

        transactionResponse["liquidityData"] = {
        "low":[],
        "medium":[],
        "high":[],
        }

        PAST_WINDOW = 10

        # Moving Average
        # emAvg = pd.Series(transactionResponse["graphData"]["token1Price"]).ewm(span = PAST_WINDOW, adjust=False).mean().iloc[-1]
        close = pd.to_numeric(responseDf["token1Price"])
        delta = close.diff()

        delta = delta[1:]
        up, down = delta.clip(lower=0), delta.clip(upper=0)

        # Calculate the EWMA
        roll_up1 = up.ewm(span=PAST_WINDOW).mean()
        roll_down1 = down.abs().ewm(span=PAST_WINDOW).mean()

        # Calculate the RSI based on EWMA
        RS1 = roll_up1 / roll_down1
        RSI1 = 100.0 - (100.0 / (1.0 + RS1))

        print("Checking RSI values")
        print(RSI1.iloc[-1])
        print(type(RSI1))


        # Strategy creation
        # Mulipliers
        LOW_BOLLINGER = 6
        MED_BOLLINGER = 4
        HIGH_BOLLINGER = 2
        PAST_WINDOW = 10

        # Moving Average
        mAvg = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(PAST_WINDOW).mean().iloc[-1]
        # STD
        std0 = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(PAST_WINDOW).std().iloc[-1]

        print("Moving averages calculated")

        print(type(transactionResponse["graphData"]["token1Price"]))

        # Latest price
        price0 = transactionResponse["graphData"]["token1Price"][-1]
        currentTick = tickcalc(price0)
        print("Current tick value")
        print(currentTick)
        print()
        # print("Got the latest price")

        # # Getting the prices and sqrt prices to "str" for JSON Serialization
        # transactionResponse["graphData"]["price"] = list(transactionResponse["graphData"]["price"].astype(str))
        # transactionResponse["graphData"]["sqrtPrice"] = list(transactionResponse["graphData"]["sqrtPrice"].astype(str))

        if(RSI1.iloc[-1]<30):
            llow = price0 - (std0 * LOW_BOLLINGER)*0.5
            lhigh = price0 * price0/(price0 - (std0 * LOW_BOLLINGER)*1.5)

            mlow = price0 - (std0 * MED_BOLLINGER)*0.5
            mhigh =price0 * price0/(price0 - (std0 * MED_BOLLINGER)*1.5)

            hlow = price0 - (std0 * HIGH_BOLLINGER)*0.5
            hhigh = price0 * price0/(price0 - (std0 * HIGH_BOLLINGER)*1.5)

        elif(RSI1.iloc[-1]>70):
            llow = price0 - (std0 * LOW_BOLLINGER)*1.5
            lhigh = price0 * price0/(price0 - (std0 * LOW_BOLLINGER)*0.5)

            mlow = price0 - (std0 * MED_BOLLINGER)*1.5
            mhigh =price0 * price0/(price0 - (std0 * MED_BOLLINGER)*0.5)

            hlow = price0 - (std0 * HIGH_BOLLINGER)*1.5
            hhigh = price0 * price0/(price0 - (std0 * HIGH_BOLLINGER)*0.5)

        else:
            # print("Checking for RSI values")
            llow = price0 - (std0 * LOW_BOLLINGER)*((RSI1.iloc[-1]*50/40 - 12.5)/50)
            lhigh = price0 * price0/(price0 - (std0 * LOW_BOLLINGER)*(2 - (RSI1.iloc[-1]*50/40 - 12.5)/50))
            # print(llow)
            # print(lhigh)

            mlow = price0 - (std0 * MED_BOLLINGER)*((RSI1.iloc[-1]*50/40 - 12.5)/50)
            mhigh = price0 * price0/(price0 - (std0 * MED_BOLLINGER)*(2 - (RSI1.iloc[-1]*50/40 - 12.5)/50))
            # print(mlow)
            # print(mhigh)

            hlow = price0 - (std0 * HIGH_BOLLINGER)*((RSI1.iloc[-1]*50/40 - 12.5)/50)
            hhigh = price0 * price0/(price0 - (std0 * HIGH_BOLLINGER)*(2 - (RSI1.iloc[-1]*50/40 - 12.5)/50))


        # currentTick = tickcalc(price0)
        llow = price0 - (std0 * LOW_BOLLINGER)*0.5 if llow<=0 else llow 
        mlow = price0 - (std0 * MED_BOLLINGER)*0.5 if mlow<=0 else mlow 
        hlow = price0 - (std0 * HIGH_BOLLINGER)*0.5 if hlow<=0 else hlow


        l_lowerTick = round(pricetickcalc(llow)/tickSpacing)*tickSpacing
        l_higherTick = round(pricetickcalc(lhigh)/tickSpacing)*tickSpacing

        # Edge case handling
        if (l_lowerTick == l_higherTick):
            llow = priceFromTick(math.floor(currentTick/tickSpacing) * tickSpacing)
            lhigh = priceFromTick(math.ceil(currentTick/tickSpacing) * tickSpacing)



        transactionResponse["liquidityData"]["low"].append(( str(max(llow, 0)), str(max(price0, 0)), str(max(lhigh, 0))))


        


        m_lowerTick = round(pricetickcalc(mlow)/tickSpacing)*tickSpacing
        m_higherTick = round(pricetickcalc(mhigh)/tickSpacing)*tickSpacing

        # Edge case handling
        if (m_lowerTick == m_higherTick):
            mlow = priceFromTick(math.floor(currentTick/tickSpacing) * tickSpacing)
            mhigh = priceFromTick(math.ceil(currentTick/tickSpacing) * tickSpacing)



        # transactionResponse["liquidityData"]["medium"].append((str(mlow), str(price0), str(mhigh)))
        transactionResponse["liquidityData"]["medium"].append(( str(max(mlow, 0)), str(max(price0, 0)), str(max(mhigh, 0))))
        

        h_lowerTick = round(pricetickcalc(hlow)/tickSpacing)*tickSpacing
        h_higherTick = round(pricetickcalc(hhigh)/tickSpacing)*tickSpacing

        # Edge case handling
        if (h_lowerTick == h_higherTick):
            hlow = priceFromTick(math.floor(currentTick/tickSpacing) * tickSpacing)
            hhigh = priceFromTick(math.ceil(currentTick/tickSpacing) * tickSpacing)

        # transactionResponse["liquidityData"]["high"].append((str(hlow), str(price0), str(hhigh) ))
        transactionResponse["liquidityData"]["high"].append(( str(max(hlow, 0)), str(max(price0, 0)), str(max(hhigh, 0))))

        if price0>=positionPriceLower and price0<=positionPriceUpper:
            transactionResponse["rebalanceRequired"] = False
        else:
            transactionResponse["rebalanceRequired"] = True





    except Exception as e:
        # If any error faced in parsing the above keys
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

        # Converting python dictionary to JSON Object
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
#             "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
#             "graph": "v3_testing",
#             "feeAmount":500,
#             "token1priceLower":0.2,
#             "token1priceUpper":0.5
#           }
#         }

#     input_json = json.dumps(input_data)
#     print("Test JSON")
#     print(input_json)
#     result = lambda_handler(input_json,"Context")
#     print(result)


#         # positionPriceLower = payload["token1priceLower"]
#         # positionPriceUpper = payload["token1priceUpper"]
