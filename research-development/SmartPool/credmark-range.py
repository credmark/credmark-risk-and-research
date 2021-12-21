# Import packages
import json
from datetime import datetime

import pandas as pd
import numpy as np
# from gql import gql, Client
# from gql.transport.requests import RequestsHTTPTransport


# # Getting the credentials for graph
# # Link for testing subgraph: https://thegraph.com/explorer/subgraph/ianlapham/uniswap-v3-testing
# GRAPH_URL_TESTING = "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-testing"


# GRAPH_API_DICT = {
#     "poolDayDatas" : [
#     ["(where: {pool: \"", "\" }){ "],
#      ["date", 
#      "sqrtPrice", 
#      "token0Price", 
#      "token1Price", 
#      "tick", 
#      "tvlUSD", 
#      "volumeToken0", 
#      "volumeToken1", 
#      "volumeUSD", 
#      "txCount",
#      "}"]

#      ]
#      }


def DateTimeConverter(input_date):
    return datetime.utcfromtimestamp(input_date).strftime('%Y-%m-%d %H:%M:%S')


def lambda_handler(event, context):


    # Response JSON
    response = {}
    transactionResponse = {}

    try:
        # Getting the payload
        payload = event["body"]


        # queryType = payload["query"]
        # poolAddress = payload["address"]
        # graphMode = payload["graph"]


        # # Other mode comparisons can be added here later
        # if (graphMode == "v3_testing"):
        #     GRAPH_URL = GRAPH_URL_TESTING
        priceHistory = payload["priceHistory"]


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
        #       priceHistory: [
        # {
        #   timestamp: "2021-06-18T15:03:00.005Z",
        #   token0Price: 0.35516
        #   token1Price: 2.81563
        # },
        pricing = pd.DataFrame.from_records(priceHistory)

        # responseDf = pd.DataFrame()

        # if (responseDf.columns[0] == "date"):
        #     responseDf['date'] = responseDf['date'].apply(DateTimeConverter)


        checkResponse = {}
        checkResponse["graphData"] = {
            "timestamp": list(pricing["timestamp"]),
            "price":list(pricing["token0Price"].astype(float))
            # "sqrtPrice":list(responseDf["sqrtPrice"].astype(str)),
            # "tick": list(responseDf["tick"]),
            # # "price":list(pow(1.0001, responseDf["tick"].astype(float))),
            # "price":[str(pow(1.0001, x)) for x in responseDf["tick"].astype(int)],
            # "token0Price": list(pd.to_numeric(responseDf["token0Price"])),
            # "token1Price": list(pd.to_numeric(responseDf["token1Price"]))
        }

        idx_to_delete = []

        # Check for NaN values in the transactionResponse graphData for price
        for i in range(len(checkResponse["graphData"]["price"])):
            if (checkResponse["graphData"]["price"][i]!=checkResponse["graphData"]["price"][i]):
                    idx_to_delete.append(i)

        if idx_to_delete:
            
            for key in checkResponse["graphData"].keys():
                updated_values = []
                for i in range(len(checkResponse["graphData"][key])):
                    if(i not in idx_to_delete):
                        updated_values.append(checkResponse["graphData"][key][i])

                checkResponse["graphData"][key] = updated_values


        # # Updating values to string
        # transactionResponse["graphData"]["date"] = pd.Series(transactionResponse["graphData"]["date"])
        # transactionResponse["graphData"]["sqrtPrice"] = pd.Series(transactionResponse["graphData"]["sqrtPrice"]).astype(str)
        # transactionResponse["graphData"]["tick"] = pd.Series(transactionResponse["graphData"]["tick"]).astype(str)
        # transactionResponse["graphData"]["price"] = pd.Series(transactionResponse["graphData"]["price"]).astype(str)


        transactionResponse["liquidityData"] = {
        "low":[],
        "medium":[],
        "high":[],
        }

        # Strategy creation
        # Mulipliers
        LOW_BOLLINGER = 6
        PAST_WINDOW = 5760
        # Moving Average
        mAvg = pd.Series(checkResponse["graphData"]["price"]).rolling(PAST_WINDOW).mean().iloc[-1]
        # STD
        std0 = pd.Series(checkResponse["graphData"]["price"]).rolling(PAST_WINDOW).std().iloc[-1]

        # Latest price
        price0 = checkResponse["graphData"]["price"][-1]
        # Low-risk strategy
        llow = price0 - std0 * LOW_BOLLINGER
        hlow = price0 * (price0/llow)

        transactionResponse["liquidityData"]["low"].append((str(llow), str(price0), str(hlow) ))


        # transactionResponse["graphData"]["token0Price"] = pd.Series(transactionResponse["graphData"]["token0Price"]).astype(str)
        # transactionResponse["graphData"]["token1Price"] = pd.Series(transactionResponse["graphData"]["token1Price"]).astype(str)




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

        response["body"] =  json.dumps(transactionResponse)

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



    




