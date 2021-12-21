# Import packages
import json
from datetime import datetime

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


def DateTimeConverter(input_date):
    return datetime.utcfromtimestamp(input_date).strftime('%Y-%m-%d %H:%M:%S')


def lambda_handler(event, context):


    # Response JSON
    response = {}
    transactionResponse = {}

    try:
        # Getting the payload
        payload = event["body"]


        queryType = payload["query"]
        poolAddress = payload["address"]
        graphMode = payload["graph"]


        # Other mode comparisons can be added here later
        if (graphMode == "v3_testing"):
            GRAPH_URL = GRAPH_URL_TESTING


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
        print("Checking query string")
        print(type(query_str))
        print(query_str)

        # Creating the actual query
        query = gql(query_str)

        queryResponse = client.execute(query)

        # Creating dataframe for the respose
        responseDf = pd.DataFrame(queryResponse[queryType])

        if (responseDf.columns[0] == "date"):
            responseDf['date'] = responseDf['date'].apply(DateTimeConverter)

        transactionResponse["graphData"] = {
            "date": list(responseDf["date"]),
            "sqrtPrice":list(responseDf["sqrtPrice"]),
            "tick": list(responseDf["tick"]),
            "price":list(responseDf["sqrtPrice"].astype(np.float128) * responseDf["sqrtPrice"].astype(np.float128))
        }


        transactionResponse["liquidityData"] = {
        "low":[],
        "medium":[],
        "high":[],
        }

        # Strategy creation
        # Mulipliers
        LOW_BOLLINGER = 6
        MED_BOLLINGER = 4
        HIGH_BOLLINGER = 2
        PAST_WINDOW = 10

        # Moving Average
        mAvg = pd.Series(transactionResponse["graphData"]["price"]).rolling(PAST_WINDOW).mean().iloc[-1]
        # STD
        std0 = pd.Series(transactionResponse["graphData"]["price"]).rolling(PAST_WINDOW).std().iloc[-1]

        # Latest price
        price0 = transactionResponse["graphData"]["price"][-1]
        # Low-risk strategy
        transactionResponse["liquidityData"]["low"].append((price0 - std0 * LOW_BOLLINGER, price0, price0 + std0 * LOW_BOLLINGER ))
        # Medium-risk strategy
        transactionResponse["liquidityData"]["medium"].append((price0 - std0 * MED_BOLLINGER, price0, price0 + std0 * MED_BOLLINGER ))
        # High-risk strategy
        transactionResponse["liquidityData"]["high"].append((price0 - std0 * HIGH_BOLLINGER, price0, price0 + std0 * HIGH_BOLLINGER ))





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



    




