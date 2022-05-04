# Documentation

import json

# For techical Analysis
import numpy as np
import ta
import pandas as pd
import requests
from pprint import pprint
from ticker_list import TICKER_LIST


TICKER_LIST = ["AAPL", "KO", "GME", "REC"]


def collect_ticker_indexes(ticker):
    json_file_name = 'marketstack_example_data.json'    
    # test block to read from marketstack.
    # NOTE the free account does not support https requests
    print(ticker)
    if ticker['mic'] == 'XMIL':
        symbols = '{}.{}'.format(ticker['symbol'], ticker['mic'])
    else:
        symbols = ticker['symbol']

    params_1 = {
        'access_key': 'efe44c8cd13833d38f1e49d26699bbab',
        'exchange': 'XNAS',
        'limit': 100
    }

    params = {
        'access_key': 'efe44c8cd13833d38f1e49d26699bbab',
        'symbols': symbols,
        'exchange': ticker['mic'],
        'limit': 100
    }

    #url_1 = 'http://api.marketstack.com/v1/tickers/{}/eod'.format(ticker_symbol.lower())
    url = 'http://api.marketstack.com/v1/eod'.format(ticker['symbol'].lower())

    api_result = requests.get(url, params)
    api_response = api_result.json()
    
    ##################################################
    
    # test block to write the api response to a json file    
    
    jsonString = json.dumps(api_response)
    jsonFile = open(json_file_name, "w")
    jsonFile.write(jsonString)
    jsonFile.close()

    ###################################################

    # test block to read from a file instead of callign api

    

    json_file = open(json_file_name)
    api_response = json.load(json_file)

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
        "Partition_key": "{}:{}:{}".format(ticker_market, ticker_exchange, ticker_symbol),
        "sort_key": "DAY_{}".format(ticker_date),
        "name": ticker_name,
        "open": ticker_open,
        "close": ticker_close,
        "high": ticker_high,
        "low": ticker_low,
        "volume": ticker_volume,
        "mfi_14": mfi_14,
        "rfi_14": rsi_14,
        "w_14": w_14,
        "atr_14": atr_14,
        "adx_14": adx_14,
        "adx_pos_14": adx_pos_14,
        "adx_neg_14": adx_neg_14,
        "zatrm_14": zATRm,
        "atrm": aTRm,
        "eva": eva,
        "wally_index": wall_y_index,
        "sx": sx,
        "DiX": dix,
        "vor": vor,
    }

    return ticker_value

#def data_collector(event, context):

def main(event, context):
    for ticker in TICKER_LIST:
        test = collect_ticker_indexes(ticker)
        print(test)

def main1(event, context):

    json_file_name = 'marketstack_example_data.json'    
    # test block to read from marketstack.
    # NOTE the free account does not support https requests

    params = {
        'access_key': 'efe44c8cd13833d38f1e49d26699bbab',
        'limit': 100
    }

    api_result = requests.get('http://api.marketstack.com/v1/tickers/aapl/eod', params)
    api_response = api_result.json()
    
    ##################################################
    
    # test block to write the api response to a json file    
    
    jsonString = json.dumps(api_response)
    jsonFile = open(json_file_name, "w")
    jsonFile.write(jsonString)
    jsonFile.close()

    ###################################################

    # test block to read from a file instead of callign api

    

    json_file = open(json_file_name)
    api_response = json.load(json_file)

    ###################################################

    # storing the end of the day data in a dataframe
    local_eod = api_response['data']['eod']
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
    
    # our indicators
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
    
    #print('Stock Name: {}, has EOD: {}'.format(local_data['data']['name'], local_data['data']['has_eod'] ))
    #pprint(panda_df.close[0:15])
    print('\n -------- PANDA -------- \n')
    #print(panda_df)
    # print('\n --------- HIGH --------- \n')
    # print(panda_df["high"])
    # print('\n ---------- LOW ------------- \n')
    # pprint(panda_df["low"])
    # print('\n ---------- CLOSE ------------- \n')
    #pprint(panda_df["close"])
    pprint(close)
    # print('\n --------- VOLUME -------------- \n')
    # pprint(panda_df["volume"])
    print('\n ------- MFI --------- \n')
    #pprint(mfi.money_flow_index())
    pprint(mfi_14)
    print('\n ------- RSI --------- \n')
    #print(rsi.rsi())
    print(rsi_14)
    # outFile = open("output-rsi", "w")
    # outFile.write('\n ------- Valori chiusura oggi --------- \n')
    # outFile.write(panda_df['close'].to_string())    
    
    # outFile.write('\n ------- RSI --------- \n')
    # outFile.write(rsi.rsi().to_string())
    # outFile.close()
    
    # pprint(rsi_14)
    print('\n ------- Williams --------- \n')
    pprint(w_14)
    print('\n ------- ATR --------- \n')
    pprint(atr_14)
    print('\n ------- ADX --------- \n')
    pprint('ADX: {}, ADX Pos: {}, ADX Neg: {}'.format(adx_14, adx_pos_14, adx_neg_14))
    print('\n ------- Our Indexes --------- \n')
    pprint('zATRM: {}, aTRm: {}'.format(zATRm, aTRm))
    print('\n ------------------------------------------------------------ \n')
    pprint('EVA: {}, Wally-index: {}, SX: {}'.format(eva, wall_y_index, sx))
    pprint('DiX: {}, VoR: {}'.format(dix, vor))    
    

    ticker_value = {
        "Partition_key": "{}:{}".format(ticker_exchange, ticker_symbol),
        "sort_key": "DAY_{}".format(ticker_date),
        "open": ticker_open,
        "close": ticker_close,
        "high": ticker_high,
        "low": ticker_low,
        "volume": ticker_volume,
        "mfi_14": mfi_14,
        "rfi_14": rsi_14,
        "w_14": w_14,
        "atr_14": atr_14,
        "adx_14": adx_14,
        "adx_pos_14": adx_pos_14,
        "adx_neg_14": adx_neg_14,
        "zatrm_14": zATRm,
        "atrm": aTRm,
        "eva": eva,
        "wally_index": wall_y_index,
        "sx": sx,
        "DiX": dix,
        "vor": vor,
    }

    pprint(ticker_value)


    a = np.arange(15).reshape(3, 5)
 
    # print("Your numpy array:")
    # print(a)
    
    
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event,
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """


if __name__ == "__main__":
    main('', '')
