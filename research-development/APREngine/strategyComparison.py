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
    ["(where: {pool: \"", "\" ,  date_lt: ", " date_gt: "," }){ "],
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


class BollingerDayStrategy:
    '''
        Bollinger Band position recommendation and rebalancing
    '''

    def __init__(self, tradingFrequency, tickSpacing, startDate):

        # Base parameters
        self.mavg = 0
        self.std0 = 0
        self.price0 = 0
        self.currentTick = 0

        self.LOW_BOLLINGER = 6
        self.MED_BOLLINGER = 4
        self.HIGH_BOLLINGER = 2
        self.PAST_WINDOW = 10

        # Maximum trades per week
        self.tradingFrequency = tradingFrequency
        self.activeLiquidityPeriod = {"low":0, "medium":0, "high":0}
        self.inactiveDays = {"low":0, "medium":0, "high":0}
        self.numberOfTransactions = {"low":0, "medium":0, "high":0}
        self.totalDays = 0
        self.tickSpacing = tickSpacing
        self.counterDays = 0
        self.date = None
        
        
        self.startDate = startDate

        self.liquidityData = {
        "low": [],
        "medium": [],
        "high": []
        }

        self.currentPosition = {
        "low": {"positionPriceLower": 0, "positionPriceUpper":0},
        "medium": {"positionPriceLower": 0, "positionPriceUpper":0},
        "high": {"positionPriceLower": 0, "positionPriceUpper":0}
        }

        self.lastTradingDay = {
            "low":-10,
            "medium": -10,
            "high": -10
        }

        self.recommendedPositions = {

            "low":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                },
            "medium":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                },
            "high":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                }


        }
    def needsRebalancing(self, DAY_COUNTER):


        res = []
        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["low"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["low"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["medium"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["medium"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["high"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["high"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        return res
    # Creating position recommendation and rebalancer within the same one
    def positionRecommender(self, DAY_COUNTER):

        positionResponse = {
            "liquidityData":
            {
                "low":[],
                "medium":[],
                "high":[]
            }
        }

        currentTick = tickcalc(self.price0[DAY_COUNTER])
        print("Current tick value")
        print(currentTick)
        print(DAY_COUNTER)
        print("Test price")
        print(self.price0[DAY_COUNTER])
        print("Test Done")
        # print("Got the latest price")

        # # Getting the prices and sqrt prices to "str" for JSON Serialization
        # transactionResponse["graphData"]["price"] = list(transactionResponse["graphData"]["price"].astype(str))
        # transactionResponse["graphData"]["sqrtPrice"] = list(transactionResponse["graphData"]["sqrtPrice"].astype(str))

        # Low-risk strategy
        llow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.LOW_BOLLINGER
        lhigh = self.price0[DAY_COUNTER] + self.std0[DAY_COUNTER] * self.LOW_BOLLINGER
        # lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/llow



        llow = self.price0[DAY_COUNTER] - ( self.price0[DAY_COUNTER]  /32 ) if llow<=0 else llow 
        

        l_lowerTick = round(pricetickcalc(llow)/self.tickSpacing)*self.tickSpacing
        l_higherTick = round(pricetickcalc(lhigh)/self.tickSpacing)*self.tickSpacing




        print("Lower values done")

        # Edge case handling
        if (l_lowerTick == l_higherTick):
            llow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            lhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)



        # positionResponse["liquidityData"]["low"].append(( str(llow), str(self.price0[DAY_COUNTER]), str(lhigh)))
        positionResponse["liquidityData"]["low"].append(( str(max(llow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(lhigh, 0))))

        # Medium-risk strategy
        mlow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.MED_BOLLINGER
        # mhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/mlow
        mhigh = self.price0[DAY_COUNTER] + self.std0[DAY_COUNTER] * self.MED_BOLLINGER
        mlow = self.price0[DAY_COUNTER] - ( self.price0[DAY_COUNTER]  /32 ) if mlow<=0 else mlow 
        

        m_lowerTick = round(pricetickcalc(mlow)/self.tickSpacing)*self.tickSpacing
        m_higherTick = round(pricetickcalc(mhigh)/self.tickSpacing)*self.tickSpacing

        # Edge case handling
        if (m_lowerTick == m_higherTick):
            mlow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            mhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)



        # positionResponse["liquidityData"]["medium"].append((str(mlow), str(self.price0[DAY_COUNTER]), str(mhigh)))
        positionResponse["liquidityData"]["medium"].append(( str(max(mlow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(mhigh, 0))))
        print("Medium values done")

        # High-risk strategy
        hlow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER
        hhigh = self.price0[DAY_COUNTER] + self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER
        # hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/hlow
        hlow = self.price0[DAY_COUNTER] - ( self.price0[DAY_COUNTER]  /32 ) if hlow<=0 else hlow 

        h_lowerTick = round(pricetickcalc(hlow)/self.tickSpacing)*self.tickSpacing
        h_higherTick = round(pricetickcalc(hhigh)/self.tickSpacing)*self.tickSpacing

        # Edge case handling
        if (h_lowerTick == h_higherTick):
            hlow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            hhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)

        # positionResponse["liquidityData"]["high"].append((str(hlow), str(self.price0[DAY_COUNTER]), str(hhigh) ))
        positionResponse["liquidityData"]["high"].append(( str(max(hlow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(hhigh, 0))))
        print("High values done")

        return positionResponse
    def simulate(self):
        
        for i in range(self.PAST_WINDOW-1, self.totalDays-1):
            self.counterDays += 1
            # print("Current day")
            # print(i)
            rebalance = self.needsRebalancing(i+1)
            if(rebalance[0] or rebalance[1] or rebalance[2]):
                # print("Rebalancer called")
                pos = self.positionRecommender(i+1)
                # print(pos)


                if(rebalance[0]):
                    self.inactiveDays["low"] += 1


                    if ((self.counterDays - self.lastTradingDay["low"])>= self.tradingFrequency) and datetime.strptime(self.startDate, '%d-%m-%Y') <= datetime.strptime(self.date[i],'%Y-%m-%d  %H:%M:%S'):     
                        print("%%%%%%%%%%%% " ,self.date[i])
                        self.lastTradingDay["low"] = self.counterDays
                        self.currentPosition["low"]["positionPriceLower"] = pos["liquidityData"]["low"][0][0]
                        self.currentPosition["low"]["positionPriceUpper"] = pos["liquidityData"]["low"][0][2]
                        self.recommendedPositions["low"]["day"].append(i+1)
                        self.recommendedPositions["low"]["positionPriceLower"].append(self.currentPosition["low"]["positionPriceLower"])
                        self.recommendedPositions["low"]["positionPriceUpper"].append(self.currentPosition["low"]["positionPriceUpper"])
                        self.recommendedPositions["low"]["date"].append(self.date[i])
                        self.numberOfTransactions["low"] += 1
                    


                if(rebalance[1]):
                    self.inactiveDays["medium"] += 1

                    if ((self.counterDays - self.lastTradingDay["medium"])>= self.tradingFrequency) and datetime.strptime(self.startDate, '%d-%m-%Y') <= datetime.strptime(self.date[i],'%Y-%m-%d  %H:%M:%S'):     
                        self.lastTradingDay["medium"] = self.counterDays
                        self.currentPosition["medium"]["positionPriceLower"] = pos["liquidityData"]["medium"][0][0]
                        self.currentPosition["medium"]["positionPriceUpper"] = pos["liquidityData"]["medium"][0][2]
                        self.recommendedPositions["medium"]["day"].append(i+1)
                        self.recommendedPositions["medium"]["positionPriceLower"].append(self.currentPosition["medium"]["positionPriceLower"])
                        self.recommendedPositions["medium"]["positionPriceUpper"].append(self.currentPosition["medium"]["positionPriceUpper"])
                        self.recommendedPositions["medium"]["date"].append(self.date[i])
                        self.numberOfTransactions["medium"] += 1

                if(rebalance[2]):
                    self.inactiveDays["high"] += 1

                    if ((self.counterDays - self.lastTradingDay["high"])>= self.tradingFrequency) and datetime.strptime(self.startDate, '%d-%m-%Y') <= datetime.strptime(self.date[i],'%Y-%m-%d  %H:%M:%S'):     
                        
                        self.lastTradingDay["high"] = self.counterDays
                        self.currentPosition["high"]["positionPriceLower"] = pos["liquidityData"]["high"][0][0]
                        self.currentPosition["high"]["positionPriceUpper"] = pos["liquidityData"]["high"][0][2]
                        self.recommendedPositions["high"]["day"].append(i+1)
                        self.recommendedPositions["high"]["positionPriceLower"].append(self.currentPosition["high"]["positionPriceLower"])
                        self.recommendedPositions["high"]["positionPriceUpper"].append(self.currentPosition["high"]["positionPriceUpper"])
                        self.recommendedPositions["high"]["date"].append(self.date[i])
                        self.numberOfTransactions["high"] += 1
    def setValues(self, transactionResponse):
        
        # Calculating parameter values values
        self.mavg = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(self.PAST_WINDOW).mean()
        self.std0 = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(self.PAST_WINDOW).std()
        self.price0 = transactionResponse["graphData"]["token1Price"]
        self.price0[0] = self.price0[1]
        self.totalDays = len(self.price0)
        self.date = transactionResponse["graphData"]["date"]

        print("Values set")

class RSIDayStrategy:
    '''
        Bollinger Band position recommendation and rebalancing
    '''

    def __init__(self, tradingFrequency, tickSpacing):

        # Base parameters
        self.mavg = 0
        self.std0 = 0
        self.price0 = 0
        self.currentTick = 0
        self.date = None

        # RSI Tracker
        self.RSI1 = 0

        self.LOW_BOLLINGER = 6
        self.MED_BOLLINGER = 4
        self.HIGH_BOLLINGER = 2
        self.PAST_WINDOW = 10

        # Maximum trades per week
        self.tradingFrequency = tradingFrequency
        self.activeLiquidityPeriod = {"low":0, "medium":0, "high":0}
        self.inactiveDays = {"low":0, "medium":0, "high":0}
        self.numberOfTransactions = {"low":0, "medium":0, "high":0}
        self.totalDays = 0
        self.tickSpacing = tickSpacing
        self.counterDays = 0

        self.liquidityData = {
        "low": [],
        "medium": [],
        "high": []
        }

        self.currentPosition = {
        "low": {"positionPriceLower": 0, "positionPriceUpper":0},
        "medium": {"positionPriceLower": 0, "positionPriceUpper":0},
        "high": {"positionPriceLower": 0, "positionPriceUpper":0}
        }

        self.lastTradingDay = {
            "low":-10,
            "medium": -10,
            "high": -10
        }

        self.recommendedPositions = {

            "low":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                },
            "medium":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                },
            "high":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                }


        }



    def needsRebalancing(self, DAY_COUNTER):


        res = []
        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["low"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["low"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["medium"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["medium"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["high"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["high"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        return res


    # Creating position recommendation and rebalancer within the same one
    def positionRecommender(self, DAY_COUNTER):

        positionResponse = {
            "liquidityData":
            {
                "low":[],
                "medium":[],
                "high":[]
            }
        }

        currentTick = tickcalc(self.price0[DAY_COUNTER])
        print("Current tick value")
        print(currentTick)
        print(DAY_COUNTER)
        print("Test price")
        print(self.price0[DAY_COUNTER])
        print(self.RSI1.iloc[DAY_COUNTER])
        print("Test Done")
        # print("Got the latest price")

        # # Getting the prices and sqrt prices to "str" for JSON Serialization
        # transactionResponse["graphData"]["price"] = list(transactionResponse["graphData"]["price"].astype(str))
        # transactionResponse["graphData"]["sqrtPrice"] = list(transactionResponse["graphData"]["sqrtPrice"].astype(str))



        # RSI Recommendation
        if(self.RSI1.iloc[DAY_COUNTER]<30):
            llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*0.5
            lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*1.5)

            mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*0.5
            mhigh =self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*1.5)

            hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*0.5
            hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*1.5)

        elif(self.RSI1.iloc[DAY_COUNTER]>70):
            llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*1.5
            lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*0.5)

            mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*1.5
            mhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*0.5)

            hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*1.5
            hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*0.5)

        else:
            print("Value tester")
            print(self.std0[DAY_COUNTER])
            print(self.std0[DAY_COUNTER]*self.LOW_BOLLINGER)
            print((self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))
            print(self.price0)
            print("Value Test done")
            llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50)
            lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*(2 - (self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))

            mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50)
            mhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*(2 - (self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))

            hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50)
            hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*(2 - (self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))

        llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*0.5 if llow<=0 else llow 
        mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*0.5 if mlow<=0 else mlow 
        hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*0.5 if hlow<=0 else hlow 


        # print("Position called after RSI")
        # print(DAY_COUNTER)
        # print(llow)
        # print(lhigh)
        # print("Recommendation of lower positions done")

        # # Low-risk strategy
        # llow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.LOW_BOLLINGER
        # lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/llow

        l_lowerTick = round(pricetickcalc(llow)/self.tickSpacing)*self.tickSpacing
        l_higherTick = round(pricetickcalc(lhigh)/self.tickSpacing)*self.tickSpacing

        # print("Lower values done")

        # Edge case handling
        if (l_lowerTick == l_higherTick):
            llow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            lhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)



        # positionResponse["liquidityData"]["low"].append(( str(llow), str(self.price0[DAY_COUNTER]), str(lhigh)))
        positionResponse["liquidityData"]["low"].append(( str(max(llow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(lhigh, 0))))

        # # Medium-risk strategy
        # mlow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.MED_BOLLINGER
        # mhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/mlow


        m_lowerTick = round(pricetickcalc(mlow)/self.tickSpacing)*self.tickSpacing
        m_higherTick = round(pricetickcalc(mhigh)/self.tickSpacing)*self.tickSpacing

        # Edge case handling
        if (m_lowerTick == m_higherTick):
            mlow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            mhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)



        # positionResponse["liquidityData"]["medium"].append((str(mlow), str(self.price0[DAY_COUNTER]), str(mhigh)))
        positionResponse["liquidityData"]["medium"].append(( str(max(mlow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(mhigh, 0))))
        print("Medium values done")

        # # High-risk strategy
        # hlow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER
        # hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/hlow

        h_lowerTick = round(pricetickcalc(hlow)/self.tickSpacing)*self.tickSpacing
        h_higherTick = round(pricetickcalc(hhigh)/self.tickSpacing)*self.tickSpacing

        # Edge case handling
        if (h_lowerTick == h_higherTick):
            hlow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            hhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)

        # positionResponse["liquidityData"]["high"].append((str(hlow), str(self.price0[DAY_COUNTER]), str(hhigh) ))
        positionResponse["liquidityData"]["high"].append(( str(max(hlow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(hhigh, 0))))

        print("High values done")

        return positionResponse

    def simulate(self):
        
        for i in range(self.PAST_WINDOW-1, self.totalDays-1):
            self.counterDays += 1
            # print("Current day")
            # print(i)
            rebalance = self.needsRebalancing(i)
            if(rebalance[0] or rebalance[1] or rebalance[2]):
                # print("Rebalancer called")
                pos = self.positionRecommender(i)
                # print(pos)


                if(rebalance[0]):
                    self.inactiveDays["low"] += 1


                    if ((self.counterDays - self.lastTradingDay["low"])>= self.tradingFrequency):
                        self.lastTradingDay["low"] = self.counterDays
                        self.currentPosition["low"]["positionPriceLower"] = pos["liquidityData"]["low"][0][0]
                        self.currentPosition["low"]["positionPriceUpper"] = pos["liquidityData"]["low"][0][2]
                        self.recommendedPositions["low"]["day"].append(i+1)
                        self.recommendedPositions["low"]["positionPriceLower"].append(self.currentPosition["low"]["positionPriceLower"])
                        self.recommendedPositions["low"]["positionPriceUpper"].append(self.currentPosition["low"]["positionPriceUpper"])
                        self.recommendedPositions["low"]["date"].append(self.date[i])
                        self.numberOfTransactions["low"] += 1
                    


                if(rebalance[1]):
                    self.inactiveDays["medium"] += 1

                    if ((self.counterDays - self.lastTradingDay["medium"])>= self.tradingFrequency):
                        self.lastTradingDay["medium"] = self.counterDays
                        self.currentPosition["medium"]["positionPriceLower"] = pos["liquidityData"]["medium"][0][0]
                        self.currentPosition["medium"]["positionPriceUpper"] = pos["liquidityData"]["medium"][0][2]
                        self.recommendedPositions["medium"]["day"].append(i+1)
                        self.recommendedPositions["medium"]["positionPriceLower"].append(self.currentPosition["medium"]["positionPriceLower"])
                        self.recommendedPositions["medium"]["positionPriceUpper"].append(self.currentPosition["medium"]["positionPriceUpper"])
                        self.recommendedPositions["medium"]["date"].append(self.date[i])
                        self.numberOfTransactions["medium"] += 1

                if(rebalance[2]):
                    self.inactiveDays["high"] += 1

                    if ((self.counterDays - self.lastTradingDay["high"])>= self.tradingFrequency):
                        self.lastTradingDay["high"] = self.counterDays
                        self.currentPosition["high"]["positionPriceLower"] = pos["liquidityData"]["high"][0][0]
                        self.currentPosition["high"]["positionPriceUpper"] = pos["liquidityData"]["high"][0][2]
                        self.recommendedPositions["high"]["day"].append(i+1)
                        self.recommendedPositions["high"]["positionPriceLower"].append(self.currentPosition["high"]["positionPriceLower"])
                        self.recommendedPositions["high"]["positionPriceUpper"].append(self.currentPosition["high"]["positionPriceUpper"])
                        self.recommendedPositions["high"]["date"].append(self.date[i])
                        self.numberOfTransactions["high"] += 1

                # print("Updated current and recommendedPositions")

                # positionResponse = {
                #     "liquidityData":
                #     {
                #         "low":[],
                #         "medium":[],
                #         "high":[]
                #     }
                # }
                # positionResponse["liquidityData"]["high"].append((str(hlow), str(price0), str(hhigh) ))
                # self.currentPosition = {
                # "low": {"positionPriceLower": 0, "positionPriceUpper":0},
                # "medium": {"positionPriceLower": 0, "positionPriceUpper":0},
                # "high": {"positionPriceLower": 0, "positionPriceUpper":0}
                # }

                # self.recommendedPositions = {

                #     "low":{
                #         "day":[],
                #         "positionPriceLower": [],
                #         "positionPriceUpper": [],
                #         },
                #     "medium":{
                #         "day":[],
                #         "positionPriceLower": [],
                #         "positionPriceUpper": [],
                #         },
                #     "high":{
                #         "day":[],
                #         "positionPriceLower": [],
                #         "positionPriceUpper": [],
                #         }


                # }
    def setValues(self, transactionResponse):
        
        # Calculating parameter values values

        close = pd.Series(pd.to_numeric(transactionResponse["graphData"]["token1Price"]))
        # print("Checking type of close")
        # print(type(close))
        delta = close.diff()

        delta = delta[1:]
        up, down = delta.clip(lower=0), delta.clip(upper=0)

        # Calculate the EWMA
        roll_up1 = up.ewm(span=self.PAST_WINDOW).mean()
        roll_down1 = down.abs().ewm(span=self.PAST_WINDOW).mean()

        # Calculate the RSI based on EWMA
        RS1 = roll_up1 / roll_down1
        self.RSI1 = 100.0 - (100.0 / (1.0 + RS1))

        self.mavg = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(self.PAST_WINDOW).mean()
        self.std0 = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(self.PAST_WINDOW).std()
        self.price0 = transactionResponse["graphData"]["token1Price"]

        # To tackle the standard deviation due to zero price
        self.price0[0] = self.price0[1]
        self.totalDays = len(self.price0)
        self.date = transactionResponse["graphData"]["date"]
        print("Values set")

def dateToEpoch(val):
    d, m, y = val.split(" ")[0].split("-")
    ts= datetime(int(y), int(m),int(d), 0, 0).timestamp()
    return int(ts)
class RSIDayStrategy:
    '''
        Bollinger Band position recommendation and rebalancing
    '''

    def __init__(self, tradingFrequency, tickSpacing):

        # Base parameters
        self.mavg = 0
        self.std0 = 0
        self.price0 = 0
        self.currentTick = 0
        self.date = None

        # RSI Tracker
        self.RSI1 = 0

        self.LOW_BOLLINGER = 6
        self.MED_BOLLINGER = 4
        self.HIGH_BOLLINGER = 2
        self.PAST_WINDOW = 10

        # Maximum trades per week
        self.tradingFrequency = tradingFrequency
        self.activeLiquidityPeriod = {"low":0, "medium":0, "high":0}
        self.inactiveDays = {"low":0, "medium":0, "high":0}
        self.numberOfTransactions = {"low":0, "medium":0, "high":0}
        self.totalDays = 0
        self.tickSpacing = tickSpacing
        self.counterDays = 0

        self.liquidityData = {
        "low": [],
        "medium": [],
        "high": []
        }

        self.currentPosition = {
        "low": {"positionPriceLower": 0, "positionPriceUpper":0},
        "medium": {"positionPriceLower": 0, "positionPriceUpper":0},
        "high": {"positionPriceLower": 0, "positionPriceUpper":0}
        }

        self.lastTradingDay = {
            "low":-10,
            "medium": -10,
            "high": -10
        }

        self.recommendedPositions = {

            "low":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                },
            "medium":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                },
            "high":{
                "day":[],
                "date":[],
                "positionPriceLower": [],
                "positionPriceUpper": [],
                }


        }



    def needsRebalancing(self, DAY_COUNTER):


        res = []
        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["low"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["low"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["medium"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["medium"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        if(float(self.price0[DAY_COUNTER])>=float(self.currentPosition["high"]["positionPriceLower"]) and float(self.price0[DAY_COUNTER])<=float(self.currentPosition["high"]["positionPriceUpper"])):
            res.append(False)
        else:
            res.append(True)

        return res


    # Creating position recommendation and rebalancer within the same one
    def positionRecommender(self, DAY_COUNTER):

        positionResponse = {
            "liquidityData":
            {
                "low":[],
                "medium":[],
                "high":[]
            }
        }

        currentTick = tickcalc(self.price0[DAY_COUNTER])
        print("Current tick value")
        print(currentTick)
        print(DAY_COUNTER)
        print("Test price")
        print(self.price0[DAY_COUNTER])
        print(self.RSI1.iloc[DAY_COUNTER])
        print("Test Done")
        # print("Got the latest price")

        # # Getting the prices and sqrt prices to "str" for JSON Serialization
        # transactionResponse["graphData"]["price"] = list(transactionResponse["graphData"]["price"].astype(str))
        # transactionResponse["graphData"]["sqrtPrice"] = list(transactionResponse["graphData"]["sqrtPrice"].astype(str))



        # RSI Recommendation
        if(self.RSI1.iloc[DAY_COUNTER]<30):
            llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*0.5
            lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*1.5)

            mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*0.5
            mhigh =self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*1.5)

            hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*0.5
            hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*1.5)

        elif(self.RSI1.iloc[DAY_COUNTER]>70):
            llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*1.5
            lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*0.5)

            mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*1.5
            mhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*0.5)

            hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*1.5
            hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*0.5)

        else:
            print("Value tester")
            print(self.std0[DAY_COUNTER])
            print(self.std0[DAY_COUNTER]*self.LOW_BOLLINGER)
            print((self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))
            print(self.price0)
            print("Value Test done")
            llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50)
            lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*(2 - (self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))

            mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50)
            mhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*(2 - (self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))

            hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*((self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50)
            hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/(self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*(2 - (self.RSI1.iloc[DAY_COUNTER]*50/40 - 12.5)/50))

        llow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.LOW_BOLLINGER)*0.5 if llow<=0 else llow 
        mlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.MED_BOLLINGER)*0.5 if mlow<=0 else mlow 
        hlow = self.price0[DAY_COUNTER] - (self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER)*0.5 if hlow<=0 else hlow 


        # print("Position called after RSI")
        # print(DAY_COUNTER)
        # print(llow)
        # print(lhigh)
        # print("Recommendation of lower positions done")

        # # Low-risk strategy
        # llow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.LOW_BOLLINGER
        # lhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/llow

        l_lowerTick = round(pricetickcalc(llow)/self.tickSpacing)*self.tickSpacing
        l_higherTick = round(pricetickcalc(lhigh)/self.tickSpacing)*self.tickSpacing

        # print("Lower values done")

        # Edge case handling
        if (l_lowerTick == l_higherTick):
            llow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            lhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)



        # positionResponse["liquidityData"]["low"].append(( str(llow), str(self.price0[DAY_COUNTER]), str(lhigh)))
        positionResponse["liquidityData"]["low"].append(( str(max(llow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(lhigh, 0))))

        # # Medium-risk strategy
        # mlow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.MED_BOLLINGER
        # mhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/mlow


        m_lowerTick = round(pricetickcalc(mlow)/self.tickSpacing)*self.tickSpacing
        m_higherTick = round(pricetickcalc(mhigh)/self.tickSpacing)*self.tickSpacing

        # Edge case handling
        if (m_lowerTick == m_higherTick):
            mlow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            mhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)



        # positionResponse["liquidityData"]["medium"].append((str(mlow), str(self.price0[DAY_COUNTER]), str(mhigh)))
        positionResponse["liquidityData"]["medium"].append(( str(max(mlow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(mhigh, 0))))
        print("Medium values done")

        # # High-risk strategy
        # hlow = self.price0[DAY_COUNTER] - self.std0[DAY_COUNTER] * self.HIGH_BOLLINGER
        # hhigh = self.price0[DAY_COUNTER] * self.price0[DAY_COUNTER]/hlow

        h_lowerTick = round(pricetickcalc(hlow)/self.tickSpacing)*self.tickSpacing
        h_higherTick = round(pricetickcalc(hhigh)/self.tickSpacing)*self.tickSpacing

        # Edge case handling
        if (h_lowerTick == h_higherTick):
            hlow = priceFromTick(math.floor(currentTick/self.tickSpacing) * self.tickSpacing)
            hhigh = priceFromTick(math.ceil(currentTick/self.tickSpacing) * self.tickSpacing)

        # positionResponse["liquidityData"]["high"].append((str(hlow), str(self.price0[DAY_COUNTER]), str(hhigh) ))
        positionResponse["liquidityData"]["high"].append(( str(max(hlow, 0)), str(max(self.price0[DAY_COUNTER], 0)), str(max(hhigh, 0))))

        print("High values done")

        return positionResponse

    def simulate(self):
        
        for i in range(self.PAST_WINDOW-1, self.totalDays-1):
            self.counterDays += 1
            # print("Current day")
            # print(i)
            rebalance = self.needsRebalancing(i)
            if(rebalance[0] or rebalance[1] or rebalance[2]):
                # print("Rebalancer called")
                pos = self.positionRecommender(i)
                # print(pos)


                if(rebalance[0]):
                    self.inactiveDays["low"] += 1


                    if ((self.counterDays - self.lastTradingDay["low"])>= self.tradingFrequency):
                        self.lastTradingDay["low"] = self.counterDays
                        self.currentPosition["low"]["positionPriceLower"] = pos["liquidityData"]["low"][0][0]
                        self.currentPosition["low"]["positionPriceUpper"] = pos["liquidityData"]["low"][0][2]
                        self.recommendedPositions["low"]["day"].append(i+1)
                        self.recommendedPositions["low"]["positionPriceLower"].append(self.currentPosition["low"]["positionPriceLower"])
                        self.recommendedPositions["low"]["positionPriceUpper"].append(self.currentPosition["low"]["positionPriceUpper"])
                        self.recommendedPositions["low"]["date"].append(self.date[i])
                        self.numberOfTransactions["low"] += 1
                    


                if(rebalance[1]):
                    self.inactiveDays["medium"] += 1

                    if ((self.counterDays - self.lastTradingDay["medium"])>= self.tradingFrequency):
                        self.lastTradingDay["medium"] = self.counterDays
                        self.currentPosition["medium"]["positionPriceLower"] = pos["liquidityData"]["medium"][0][0]
                        self.currentPosition["medium"]["positionPriceUpper"] = pos["liquidityData"]["medium"][0][2]
                        self.recommendedPositions["medium"]["day"].append(i+1)
                        self.recommendedPositions["medium"]["positionPriceLower"].append(self.currentPosition["medium"]["positionPriceLower"])
                        self.recommendedPositions["medium"]["positionPriceUpper"].append(self.currentPosition["medium"]["positionPriceUpper"])
                        self.recommendedPositions["medium"]["date"].append(self.date[i])
                        self.numberOfTransactions["medium"] += 1

                if(rebalance[2]):
                    self.inactiveDays["high"] += 1

                    if ((self.counterDays - self.lastTradingDay["high"])>= self.tradingFrequency):
                        self.lastTradingDay["high"] = self.counterDays
                        self.currentPosition["high"]["positionPriceLower"] = pos["liquidityData"]["high"][0][0]
                        self.currentPosition["high"]["positionPriceUpper"] = pos["liquidityData"]["high"][0][2]
                        self.recommendedPositions["high"]["day"].append(i+1)
                        self.recommendedPositions["high"]["positionPriceLower"].append(self.currentPosition["high"]["positionPriceLower"])
                        self.recommendedPositions["high"]["positionPriceUpper"].append(self.currentPosition["high"]["positionPriceUpper"])
                        self.recommendedPositions["high"]["date"].append(self.date[i])
                        self.numberOfTransactions["high"] += 1

                # print("Updated current and recommendedPositions")

                # positionResponse = {
                #     "liquidityData":
                #     {
                #         "low":[],
                #         "medium":[],
                #         "high":[]
                #     }
                # }
                # positionResponse["liquidityData"]["high"].append((str(hlow), str(price0), str(hhigh) ))
                # self.currentPosition = {
                # "low": {"positionPriceLower": 0, "positionPriceUpper":0},
                # "medium": {"positionPriceLower": 0, "positionPriceUpper":0},
                # "high": {"positionPriceLower": 0, "positionPriceUpper":0}
                # }

                # self.recommendedPositions = {

                #     "low":{
                #         "day":[],
                #         "positionPriceLower": [],
                #         "positionPriceUpper": [],
                #         },
                #     "medium":{
                #         "day":[],
                #         "positionPriceLower": [],
                #         "positionPriceUpper": [],
                #         },
                #     "high":{
                #         "day":[],
                #         "positionPriceLower": [],
                #         "positionPriceUpper": [],
                #         }


                # }
    def setValues(self, transactionResponse):
        
        # Calculating parameter values values

        close = pd.Series(pd.to_numeric(transactionResponse["graphData"]["token1Price"]))
        # print("Checking type of close")
        # print(type(close))
        delta = close.diff()

        delta = delta[1:]
        up, down = delta.clip(lower=0), delta.clip(upper=0)

        # Calculate the EWMA
        roll_up1 = up.ewm(span=self.PAST_WINDOW).mean()
        roll_down1 = down.abs().ewm(span=self.PAST_WINDOW).mean()

        # Calculate the RSI based on EWMA
        RS1 = roll_up1 / roll_down1
        self.RSI1 = 100.0 - (100.0 / (1.0 + RS1))

        self.mavg = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(self.PAST_WINDOW).mean()
        self.std0 = pd.Series(transactionResponse["graphData"]["token1Price"]).rolling(self.PAST_WINDOW).std()
        self.price0 = transactionResponse["graphData"]["token1Price"]

        # To tackle the standard deviation due to zero price
        self.price0[0] = self.price0[1]
        self.totalDays = len(self.price0)
        self.date = transactionResponse["graphData"]["date"]
        print("Values set")


def dateToEpoch(val):
    d, m, y = val.split(" ")[0].split("-")
    ts= datetime(int(y), int(m),int(d), 0, 0).timestamp()
    return int(ts)

def lambda_handler(event, context):

    # print("handler called")
    # Response JSON
    response = {}
    transactionResponse = {}

    try:
        # Getting the payload
        # event = json.loads(event)
        payload = event["body"]
        startDate = payload['startDate']
        endDate = payload['endDate']
        # print(type(payload))

        queryType = payload["query"]
        poolAddress = payload["address"]
        graphMode = payload["graph"]
        feeAmount = payload["feeAmount"]
        tickSpacing = feeMapping[str(feeAmount)]
        tradeFrequency = payload["tradeFrequency"]

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
        # query_str_list = [
        # "query {", 
        # str(queryType)+ str(GRAPH_API_DICT[queryType][0][0])+str(poolAddress)+str(GRAPH_API_DICT[queryType][0][1])
        # ]
        PAST_WINDOW=10
        datelt = dateToEpoch(endDate)#-24*3600*PAST_WINDOW
        dategt=dateToEpoch(startDate)-(24*3600*(PAST_WINDOW+20))
        query_str_list = [
        "query {", 
        str(queryType)+ str(GRAPH_API_DICT[queryType][0][0])+str(poolAddress)+str(GRAPH_API_DICT[queryType][0][1])+str(datelt)+str(GRAPH_API_DICT[queryType][0][2])+str(dategt)+str(GRAPH_API_DICT[queryType][0][3])
        ]
        print(queryType)
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

        



        # Strategy Section - Bollinger
        bollinger1 = BollingerDayStrategy(tradeFrequency, tickSpacing, payload['startDate'])
        bollinger1.setValues(transactionResponse)

        bollinger1.simulate()
        transactionResponse["bollingerPositionRecommendations"] = bollinger1.recommendedPositions
        transactionResponse["bollingerTransactions"] = {
            "low":bollinger1.numberOfTransactions["low"],
            "medium":bollinger1.numberOfTransactions["medium"],
            "high":bollinger1.numberOfTransactions["high"]
        }

        transactionResponse["bollingerActiveLiquidityPeriod"] = {
            "low": (bollinger1.counterDays - bollinger1.inactiveDays["low"]+1)*100/bollinger1.counterDays,
            "medium":(bollinger1.counterDays - bollinger1.inactiveDays["medium"]+1)*100/bollinger1.counterDays,
            "high":(bollinger1.counterDays - bollinger1.inactiveDays["high"]+1)*100/bollinger1.counterDays

        }

        # print("Bollinger response test")
        # print(transactionResponse["bollingerTransactions"])
        # print(transactionResponse["bollingerActiveLiquidityPeriod"])
        # print(transactionResponse["bollingerPositionRecommendations"])



        # Strategy Section - RSI
        RSI1 = RSIDayStrategy(tradeFrequency, tickSpacing)
        RSI1.setValues(transactionResponse)

        RSI1.simulate()
        transactionResponse["RSIPositionRecommendations"] = RSI1.recommendedPositions
        transactionResponse["RSITransactions"] = {
            "low":RSI1.numberOfTransactions["low"],
            "medium":RSI1.numberOfTransactions["medium"],
            "high":RSI1.numberOfTransactions["high"]
        }

        transactionResponse["RSIActiveLiquidityPeriod"] = {
            "low": (RSI1.counterDays - RSI1.inactiveDays["low"]+1)*100/RSI1.counterDays,
            "medium":(RSI1.counterDays - RSI1.inactiveDays["medium"]+1)*100/RSI1.counterDays,
            "high":(RSI1.counterDays - RSI1.inactiveDays["high"]+1)*100/RSI1.counterDays

        }

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





# if __name__ == "__main__":

#     # Trade Frequency per week - 1
#     input_data = {
#           "body": {
#             "query": "poolDayDatas",
#             "address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
#             "graph": "v3_testing",
#             "feeAmount":3000,
#             "tradeFrequency": 1,
#             "startDate": "10-07-2021",
#             "endDate": "10-08-2021",
#           }
#         }

#     input_json = json.dumps(input_data)
#     # print("Test JSON")
#     # print(input_json)
#     result = lambda_handler(input_data,"Context")
#     print("Lambda Result", type(result))
#     print(result)
#     with open("result.json", "w") as outfile:
#         json.dump(result, outfile)

























