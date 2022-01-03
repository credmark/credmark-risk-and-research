import os
import json
import boto3
import requests
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime

def main():
    AWS_ACCESS_KEY_ID = 'AKIAWBSHBGMZCRHWP3MQ'
    AWS_SECRET_ACCESS_KEY = "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"

    client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    bucket_name = 'varhistoricaldata'
    object_key  = 'aave-historical-dec-20.csv'
    csv_obj     = client.get_object(Bucket=bucket_name, Key=object_key)
    body        = csv_obj['Body']
    csv_string  = body.read().decode('utf-8')
    df          = pd.read_csv(StringIO(csv_string))

    errors = []
    transactionResponse  = {}
    transactionResponse['headers']        = {"Content-Type": "application/json"}
    transactionResponse                   = {"result":{"AAVEV2":{"data":{}}}}
    transactionResponse["result"]["AAVEV2"]["context"] = {}
    transactionResponse["result"]["AAVEV2"]["context"]["errors"] = errors

    transactionResponse["result"]["AAVEV2"]["data"] = []
    for row in df.iterrows():
        temp = {}
        temp['10_day_99p']               = str(row[1]['VAR'])
        temp['var_date_10_day_99p']      = row[1]['VAR_DATE']
        temp['total_assets']             = row[1]['Assets']
        temp['timestamp']                = row[1]['timestamp']
        temp['total_liabilities']        = row[1]['Liabilities']
        temp['date']                     = row[1]['date']
        temp['10_day_95p']               = None
        temp['var_date_10_day_95p']      = None
        temp['1_day_99p']                = None
        temp['var_date_1_day_99p']       = None
        temp['1_day_95p']                = None
        temp['var_date_1_day_95p']       = None
        temp['relative_var_assets']      = row[1]['VAR'] * pow(10, 9) / row[1]['Assets']
        temp['relative_var_liabilities'] = row[1]['VAR'] * pow(10, 9) /  row[1]['Liabilities']
        transactionResponse["result"]["AAVEV2"]["data"].append(temp)

    return transactionResponse



def lambda_handler(event, context):
    print("Handler called")
    response = {}
    transactionResponse = {}
    try:
        # event           = json.loads(event)
        payload         = event["body"]
        transactionResponse  = main()
        

    except Exception as e:

        s = str(e)
        response["statusCode"] = 400
        transactionResponse["error"] = True
        transactionResponse["message"] = s
        response["headers"] = {}
        response["headers"]["Content-Type"] = "application/json"
        response["response"] = transactionResponse
        response_JSON = response
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
#                         }
#                 }
#     input_json = json.dumps(input_data)
#     result = lambda_handler(input_data,"Context")
#     print(result)