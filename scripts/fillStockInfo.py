# -*- coding:utf-8 -*-

import pandas as pd
import yfinance as yf
import time
import boto3
import botocore

DYNAMO_DB_ENDPOINT = "https://dynamodb.us-west-2.amazonaws.com"
TABLE_NAME = 'stock_info'

def russell3000():
    file = '../assets/Russell3000-GICS.xlsx'
    df = pd.read_excel(file)
    return df[['symbol', 'name', 'sector', 'industry']]

def putStockInfo(df, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url=DYNAMO_DB_ENDPOINT)

    table = dynamodb.Table(TABLE_NAME)
    with table.batch_writer() as batch:
        for index, row in df.iterrows():
            # Yahoo Finance stores special tickers with '-'. Example: 'BF-A' instead of 'BF/A'.
            symbol = row['symbol']
            symbol = symbol.replace('/','-')
            batch.put_item(
                Item={
                    'ticker': symbol,
                    'data': {
                        'name': row['name'],
                        'sector': row['sector'],
                        'industry': row['industry']
                    }
                }
            )

if __name__ == "__main__":
    dynamodb = boto3.resource('dynamodb', endpoint_url=DYNAMO_DB_ENDPOINT)
    df = russell3000()
    putStockInfo(df, dynamodb)
