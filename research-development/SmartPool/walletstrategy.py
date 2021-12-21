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
# GRAPH_URL_TESTING = "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-testing"
GRAPH_URL_TESTING = "https://api.thegraph.com/subgraphs/name/benesjan/uniswap-v3-subgraph"


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

     ],

     "poolHourDatas" : [
     ["(where: {pool: \"", "\" }){ "],
     [
     "sqrtPrice",
     "liquidity",
     "periodStartUnix", 
     "token0Price", 
     "token1Price", 
     "tick", 
     "tvlUSD",
     "open",
     "high",
     "low",
     "close", 
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



class AlphaVaultsStrategy:
    '''
        Alpha Vault's Charm Finance Strategy Simulation
    '''


    def __init__(self, tradingFrequency, tickSpacing):

        # Base Parameters
        self.tickSpacing = tickSpacing
        self.tradingFrequency = tradingFrequency
        self.date = None
        self.price0 = None
        self.inactiveDays = {"baseOrder":{"low": 0, "medium": 0, "high": 0}, "rangeOrder": {"low":0, "medium":0, "high": 0}}
        self.numberOfRecommendations = {"baseOrder":{"low": 0, "medium": 0, "high": 0}, "rangeOrder": {"low":0, "medium":0, "high": 0}}
        self.totalDays = 0
        self.counterDays = 0


        # Base range B in terms of percentages
        self.baseRange = {0.20, 0.15, 0.10} # 20% base range order for low risk range, this number can be changed
        self.rangeOrderRange = {0.08, 0.06, 0.04} # 8% range order for low risk range

        self.token0balance = 0
        self.token1balance = 0



        self.lastTradingDay = {
            "low": -10,
            "medium": -10,
            "high": -10
        }

        self.currentPosition = {
            "low": {"baseOrder": {"positionPriceLower": 0, "positionPriceUpper": 0}, "rangeOrder": {"positionPriceLower": 0, "positionPriceUpper": 0}},
            "medium": {"baseOrder": {"positionPriceLower": 0, "positionPriceUpper": 0}, "rangeOrder": {"positionPriceLower": 0, "positionPriceUpper": 0}},
            "high": {"baseOrder": {"positionPriceLower": 0, "positionPriceUpper": 0}, "rangeOrder": {"positionPriceLower": 0, "positionPriceUpper": 0}}
        }


        self.recommendedPositions = {

            "low": {"baseOrder": {"day": [], "date": [], "positionPriceLower": [], "positionPriceUpper": []}, "rangeOrder": {"day": [], "date": [], "positionPriceLower": [], "positionPriceUpper": []}},
            "medium": {"baseOrder": {"day": [], "date": [], "positionPriceLower": [], "positionPriceUpper": []}, "rangeOrder": {"day": [], "date": [], "positionPriceLower": [], "positionPriceUpper": []}},
            "high": {"baseOrder": {"day": [], "date": [], "positionPriceLower": [], "positionPriceUpper": []}, "rangeOrder": {"day": [], "date": [], "positionPriceLower": [], "positionPriceUpper": []}}

        }




    def needsRebalancing(self, DAY_COUNTER):
        res = []
        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["low"]["baseOrder"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["low"]["baseOrder"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["medium"]["baseOrder"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["medium"]["baseOrder"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["high"]["baseOrder"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["high"]["baseOrder"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        return res



    def setValues(self,transactionResponse, token0balance, token1balance):
        
        self.token0balance = token0balance
        self.token1balance = token1balance
        
        self.date = transactionResponse["graphData"]["date"]
        self.price0 = transactionResponse["graphData"]["token1Price"]
        self.totalDays = len(self.price0)


    def positionRecommender(self, DAY_COUNTER):

        positionResponse = {
            "liquidityData":
            {
                "baseOrder": {
                    "low": [],
                    "medium": [],
                    "high": []
                },
                "rangeOrder": {
                    "low": [],
                    "medium": [],
                    "high": []
                }
            }
        }

        currentTick = tickcalc(self.price0[DAY_COUNTER])

        


    def simulate(self):
        for i in range(self.totalDays):
            self.counterDays += 1

            # Reblanace check is switched off current for this strategy as it does always center after the prescribed period
            # rebalance = self.needsRebalancing(i)
            if(self.counterDays - self.lastTradingDay["low"] >= self.tradingFrequency):
                self.lastTradingDay["low"] = self.counterDays





def lambda_handler(event, context):

    # print("handler called")
    # Response JSON
    response = {}
    transactionResponse = {}

    try:
        # Getting the payload
        event = json.loads(event)
        payload = event["body"]
        # print(type(payload))

        queryType = payload["query"]
        poolAddress = payload["address"]
        graphMode = payload["graph"]
        feeAmount = payload["feeAmount"]
        tickSpacing = feeMapping[str(feeAmount)]
        tradeFrequency = payload["tradeFrequency"]
        token0balance = payload["token0balance"]
        token1balance = payload["token1balance"]

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
        print("Checking query string")
        # print(type(query_str))
        print(query_str)

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
        else:

            transactionResponse["graphData"] = {
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

        



        # Alpha Vaults - Charm finance
        alpha1 = AlphaVaultsStrategy(tradeFrequency, tickSpacing)
        alpha1.setValues(transactionResponse, token0balance, token1balance)





        # # Strategy Section - Bollinger
        # bollinger1 = BollingerDayStrategy(tradeFrequency, tickSpacing)
        # bollinger1.setValues(transactionResponse)

        # bollinger1.simulate()
        # transactionResponse["bollingerPositionRecommendations"] = bollinger1.recommendedPositions
        # transactionResponse["bollingerTransactions"] = {
        #     "low":bollinger1.numberOfTransactions["low"],
        #     "medium":bollinger1.numberOfTransactions["medium"],
        #     "high":bollinger1.numberOfTransactions["high"]
        # }

        # transactionResponse["bollingerActiveLiquidityPeriod"] = {
        #     "low": (bollinger1.counterDays - bollinger1.inactiveDays["low"]+1)*100/bollinger1.counterDays,
        #     "medium":(bollinger1.counterDays - bollinger1.inactiveDays["medium"]+1)*100/bollinger1.counterDays,
        #     "high":(bollinger1.counterDays - bollinger1.inactiveDays["high"]+1)*100/bollinger1.counterDays

        # }

        # print("Bollinger response test")
        # print(transactionResponse["bollingerTransactions"])
        # print(transactionResponse["bollingerActiveLiquidityPeriod"])
        # print(transactionResponse["bollingerPositionRecommendations"])



        # # Strategy Section - RSI
        # RSI1 = RSIDayStrategy(tradeFrequency, tickSpacing)
        # RSI1.setValues(transactionResponse)

        # RSI1.simulate()
        # transactionResponse["RSIPositionRecommendations"] = RSI1.recommendedPositions
        # transactionResponse["RSITransactions"] = {
        #     "low":RSI1.numberOfTransactions["low"],
        #     "medium":RSI1.numberOfTransactions["medium"],
        #     "high":RSI1.numberOfTransactions["high"]
        # }

        # transactionResponse["RSIActiveLiquidityPeriod"] = {
        #     "low": (RSI1.counterDays - RSI1.inactiveDays["low"]+1)*100/RSI1.counterDays,
        #     "medium":(RSI1.counterDays - RSI1.inactiveDays["medium"]+1)*100/RSI1.counterDays,
        #     "high":(RSI1.counterDays - RSI1.inactiveDays["high"]+1)*100/RSI1.counterDays

        # }

        # print("RSI response test")
        # print(transactionResponse["RSITransactions"])
        # print(transactionResponse["RSIActiveLiquidityPeriod"])
        # print(transactionResponse["RSIPositionRecommendations"])




    except Exception as e:
        # If any error faced in parsing the above keys
        s = str(e)
        print(e)

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





if __name__ == "__main__":

    # Trade Frequency per week - 1
    input_data = {
          "body": {
            "query": "poolDayDatas",
            "address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
            "graph": "v3_testing",
            "feeAmount":3000,
            "tradeFrequency": 7,
            "token0balance":1000,
            "token1balance": 10000
          }
        }

    input_json = json.dumps(input_data)
    print("Test JSON")
    print(input_json)
    result = lambda_handler(input_json,"Context")
    # print("Lambda Result")
    print(result)

























