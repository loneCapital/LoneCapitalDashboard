# -*- coding:utf-8 -*-

import pandas as pd
import yfinance as yf
import talib
import time
import boto3
import botocore
import datetime

DYNAMO_DB_ENDPOINT = 'https://dynamodb.us-west-2.amazonaws.com'
STOCK_DATA_TABLE_NAME = 'stock_data'
STOCK_INFO_TABLE_NAME = 'stock_info'

def getDailyVolume(row):
    avg_price = (row['High'] + row['Low'] + row['Open'] + row['Close']) / 4
    daily_volume = row['Volume']
    daily_dollar_volume = daily_volume * avg_price
    return daily_volume, daily_dollar_volume

def getMonthlyVolume(df):
    month_df = df.head(22)
    monthly_volume = 0
    monthly_dollar_volume = 0
    for index, row in month_df.iterrows():
        daily_volume, daily_dollar_volume = getDailyVolume(row)
        monthly_volume += daily_volume
        monthly_dollar_volume += daily_dollar_volume
    return monthly_volume, monthly_dollar_volume

# 抵扣价 
def getDKJ(df, numOfDays):
    if (len(df) < numOfDays):
        return float("NaN")
    else:
        return df.iloc[numOfDays - 1, df.columns.get_loc('Close')]

def getStockData(ticker):
    ticker = yf.Ticker(ticker)
    hist = ticker.history(period='6mo', interval='1d')

    # calculate and fill in SMA & EMA data
    hist['SMA20'] = talib.SMA(hist['Close'], 20)
    hist['SMA60'] = talib.SMA(hist['Close'], 60)
    hist['SMA120'] = talib.SMA(hist['Close'], 120)

    hist['EMA20'] = talib.EMA(hist['Close'], 20)
    hist['EMA60'] = talib.EMA(hist['Close'], 60)
    hist['EMA120'] = talib.EMA(hist['Close'], 120)

    # reverse dataframe order
    hist = hist.iloc[::-1]

    data = {}
    # 当日开盘价，收盘价，最高价，最低价
    data['open'] = format(hist.iloc[0, hist.columns.get_loc('Open')], '.2f')
    data['open'] = format(hist.iloc[0, hist.columns.get_loc('Open')], '.2f')
    data['close'] = format(hist.iloc[0, hist.columns.get_loc('Close')], '.2f')
    data['high'] = format(hist.iloc[0, hist.columns.get_loc('High')], '.2f')
    data['low'] = format(hist.iloc[0, hist.columns.get_loc('Low')], '.2f')

    # sma(20,60,120), ema(20,60,120)
    data['sma20'] = format(hist.iloc[0, hist.columns.get_loc('SMA20')], '.2f')
    data['sma60'] = format(hist.iloc[0, hist.columns.get_loc('SMA60')], '.2f')
    data['sma120'] = format(hist.iloc[0, hist.columns.get_loc('SMA120')], '.2f')

    data['ema20'] = format(hist.iloc[0, hist.columns.get_loc('EMA20')], '.2f')
    data['ema60'] = format(hist.iloc[0, hist.columns.get_loc('EMA60')], '.2f')
    data['ema120'] = format(hist.iloc[0, hist.columns.get_loc('EMA120')], '.2f')

    # 抵扣价(20,60,120)
    data['dkj20'] = format(getDKJ(hist, 20), '.2f')
    data['dkj60'] = format(getDKJ(hist, 60), '.2f')
    data['dkj120'] = format(getDKJ(hist, 120), '.2f')

    #日成交量, 日成交额 ,月成交量, 月成交额
    daily_volume, daily_dollar_volume = getDailyVolume(hist.iloc[0])
    monthly_volume, monthly_dollar_volume = getMonthlyVolume(hist)

    data['daily_volume'] = format(daily_volume, '.2f')
    data['daily_dollar_volume'] = format(daily_dollar_volume, '.2f')
    data['monthly_volume'] = format(monthly_volume, '.2f')
    data['monthly_dollar_volume'] = format(monthly_dollar_volume, '.2f')
    return data
   
def getLastTradingDate():
    ticker = yf.Ticker('SPY')
    hist = ticker.history(period='1d', interval='1d')
    datetime_str = hist.index.values[0]
    date = datetime.datetime.strptime(f'{datetime_str}', '%Y-%m-%dT%H:%M:%S.%f000').date()
    return str(date)

def putStockData(tickers, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url=DYNAMO_DB_ENDPOINT)

    table = dynamodb.Table(STOCK_DATA_TABLE_NAME)
    date = getLastTradingDate()
    with table.batch_writer() as batch:
        count = 0
        for ticker in tickers:
            print('Progress: ' + str(count) + '/' + str(len(tickers)) + '. Ticker: ' + ticker)

            data = getStockData(ticker)
            batch.put_item(
                Item={
                    'ticker': ticker,
                    'date'  : date,
                    'data'  : data
                }
            )
            count += 1

def getAllTickers(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url=DYNAMO_DB_ENDPOINT)
    table = dynamodb.Table(STOCK_INFO_TABLE_NAME)
    response = table.scan()
    tickers = []
    for i in response['Items']:
        tickers.append(i['ticker'])
    return tickers

if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb', endpoint_url=DYNAMO_DB_ENDPOINT)
    tickers = getAllTickers(dynamodb)
    # tickers = ['BF/A']
    putStockData(tickers, dynamodb)