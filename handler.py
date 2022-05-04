# Documentation

import json

# For techical Analysis
#import numpy as np
import ta
import pandas as pd
import requests
import boto3
#from pprint import pprint
from ticker_list import TICKER_LIST

#TICKER_LIST_2 = [{'symbol':'AAPL','name':'APPLE','market':'USA','mic':'XNAS'},{'symbol':'ABT','name':'ABBOTT','market':'USA','mic':'XNYS'}]


#TICKER_LIST = ["AAPL", "KO", "GME", "REC"]


def collect_ticker_indexes(ticker):
    json_file_name = 'marketstack_example_data.json'    
    # test block to read from marketstack.
    # NOTE the free account does not support https requests

    params_1 = {
        'access_key': 'efe44c8cd13833d38f1e49d26699bbab',
        'exchange': 'XNAS',
        'limit': 100
    }

    params = {
        'access_key': 'efe44c8cd13833d38f1e49d26699bbab',
        'symbols': '{}'.format(ticker['symbol']),
        'exchange': ticker['mic'],
        'limit': 100
    }

    #url_1 = 'http://api.marketstack.com/v1/tickers/{}/eod'.format(ticker_symbol.lower())
    url = 'http://api.marketstack.com/v1/eod'

    api_result = requests.get(url, params)
    api_response = api_result.json()
    #print(api_response)
    
    ##################################################
    
    # test block to write the api response to a json file    
    
    # jsonString = json.dumps(api_response)
    # jsonFile = open(json_file_name, "w")
    # jsonFile.write(jsonString)
    # jsonFile.close()

    ###################################################

    # test block to read from a file instead of callign api

    

    # json_file = open(json_file_name)
    # api_response = json.load(json_file)

    ###################################################

    # storing the end of the day data in a dataframe
    local_eod = api_response['data']
    ticker_name = ticker['name']
    ticker_market = ticker['market']
    first_eod = local_eod[0]
    ticker_symbol= first_eod["symbol"]
    ticker_exchange= first_eod["exchange"]
    ticker_date = first_eod["date"]
    ticker_open = first_eod["open"]
    ticker_close = first_eod["close"]
    ticker_high = first_eod["high"]
    ticker_low = first_eod["low"]
    ticker_volume = first_eod["volume"]
    
    
    panda_df = pd.DataFrame(local_eod)

    #setting the date as index for the dataframe
    panda_df = panda_df.set_index(pd.DatetimeIndex(panda_df['date'].values))
    # reverting the order of the data frame to get meaningful data
    panda_df = panda_df.iloc[::-1]
    close = panda_df['close'][-1]
    volume = panda_df['volume'][-1]
    volume_mean = panda_df['volume'][-30:].mean()

    mfi = ta.volume.MFIIndicator(panda_df.high,panda_df.low,panda_df.close,panda_df.volume)
    rsi = ta.momentum.RSIIndicator(close=panda_df['close'], window=14, fillna=True)
    w = ta.momentum.WilliamsRIndicator(high=panda_df["high"],low=panda_df["low"],close=panda_df["close"],lbp=14,fillna=True)
    atr = ta.volatility.average_true_range(high=panda_df["high"],low=panda_df["low"],close=panda_df["close"],window=14,fillna=False)
    adx = ta.trend.ADXIndicator(high=panda_df["high"],low=panda_df["low"],close=panda_df["close"],window=14,fillna=False)

    mfi_14 = mfi.money_flow_index()[-1]
    rsi_14 = rsi.rsi()[-1]
    atr_14 = atr[-1]
    w_14 = w.williams_r()[-1]
    adx_14 = adx.adx()[-1]
    adx_neg_14 = adx.adx_neg()[-1]
    adx_pos_14 = adx.adx_pos()[-1]
    
    # Creating the Wally propietary indicators
    zATRm = 1 - (atr_14/close)
    aTRm = 0
    if zATRm < 0:
        aTRm = 0
    else:
        aTRm = zATRm
    eva = (((w_14+100) + rsi_14 + mfi_14)/3)*aTRm
    wall_y_index = 100 -  eva
    sx = (rsi_14/10)+25
    dix = 1
    if (adx_pos_14 - adx_neg_14) < 30: dix = -1
    vor = volume/volume_mean
    
    ticker_value = {
        'market#ticker': {
            'S': "{}#{}#{}".format(ticker_market, ticker_exchange, ticker_symbol)
            },
        'frequence#timestamp': {
            'S': "DAY#{}".format(ticker_date)
        },
        'name': {
            'S': ticker_name
        },
        'open': {
            'N' : str(round(ticker_open,4)) 
            },
        'close': {
            'N': str(round(ticker_close,4))
            },
        'high': {
            'N': str(round(ticker_high,4))
            },
        'low': {
            'N': str(round(ticker_low,4))
            },
        'volume': {
            'N': str(round(ticker_volume,4))
            },
        'mfi_14': {
            'N': str(round(mfi_14,4))
            },
        'rfi_14': {
            'N': str(round(rsi_14,4)) 
            },
        'w_14': {
            'N': str(round(w_14,4))
            },
        'atr_14': {
            'N': str(round(atr_14,4))
            },
        'adx_14': {
            'N': str(round(adx_14,4))
            },
        'adx_pos_14': {
            'N': str(round(adx_pos_14,4))
            },
        'adx_neg_14': {
            'N': str(round(adx_neg_14,4))
            },
        'zatrm_14': {
            'N': str(round(zATRm,4))
            },
        'atrm': {
            'N': str(round(aTRm,4))
            },
        'eva': {
            'N': str(round(eva,4))
            },
        'wally_index': {
            'N': str(round(wall_y_index,4))
            },
        'sx': {
            'N': str(round(sx,4))
            },
        'dix': {
            'N': str(round(dix,4))
            },
        'vor': {
            'N': str(round(vor,4))
            }
    } 

    return ticker_value

def data_collector(event, context):
    dynamodb = boto3.client('dynamodb')
    for ticker in TICKER_LIST:
        try:
            test = collect_ticker_indexes(ticker)
            print(test)
            dynamodb.put_item(TableName='financialDataItems', Item=test)
        except:
            print("cannot process ticker {}".format(ticker))


