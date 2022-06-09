# Documentation

import datetime
from logging import exception
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# For techical Analysis
#import numpy as np
import json
import ta
import pandas as pd
import requests
import boto3
#from pprint import pprint
from ticker_list import TICKER_LIST




# Calcualte the needed financial indexes
# it accept a list of object with open cloes values
# returns a formatted object ready for the DDB insertion 
def calculate_financial_index(financial_eod_data):
    ticker_symbol= financial_eod_data["symbol"]
    ticker_exchange= financial_eod_data["exchange"]
    ticker_date = financial_eod_data["date"]
    ticker_open = financial_eod_data["open"]
    ticker_close = financial_eod_data["close"]
    ticker_high = financial_eod_data["high"]
    ticker_low = financial_eod_data["low"]
    ticker_volume = financial_eod_data["volume"]

    panda_df = pd.DataFrame(financial_eod_data)

    #setting the date as index for the dataframe
    panda_df = panda_df.set_index(pd.DatetimeIndex(panda_df['date'].values))
    # reverting the order of the data frame to get meaningful data
    panda_df = panda_df.iloc[::-1]
    close = panda_df['close'][-1]
    volume = panda_df['volume'][-1]
    volume_mean = panda_df['volume'][-30:].mean()

    mfi = ta.volume.MFIIndicator(panda_df.high, panda_df.low, panda_df.close, panda_df.volume)
    rsi = ta.momentum.RSIIndicator(close=panda_df['close'], window=14, fillna=True)
    w = ta.momentum.WilliamsRIndicator(high=panda_df["high"], low=panda_df["low"], close=panda_df["close"], lbp=14, fillna=True)
    atr = ta.volatility.average_true_range(high=panda_df["high"], low=panda_df["low"], close=panda_df["close"], window=14, fillna=False)
    adx = ta.trend.ADXIndicator(high=panda_df["high"], low=panda_df["low"], close=panda_df["close"], window=14, fillna=False)

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
        'market_ticker': {
            'S': "{}_{}_{}".format(ticker_market, ticker_exchange, ticker_symbol)
            },
        'frequence_timestamp': {
            'S': "DAY_{}".format(ticker_date)
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
        'wally_index_14': {
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










def collect_ticker_indexes(ticker,date=None):
    json_file_name = 'marketstack_example_data.json'    
    # test block to read from marketstack.
    # NOTE the free account does not support https requests
    params = {
        'access_key': '3a48a0a0c36e1a36e0278d1e64aeb250',
        'symbols': '{}'.format(ticker['symbol']),
        'exchange': ticker['mic'],
        'limit': 100
        }

    if date:
        params = {
            'access_key': '3a48a0a0c36e1a36e0278d1e64aeb250',
            'symbols': '{}'.format(ticker['symbol']),
            'exchange': ticker['mic'],
            'date_from': date,
            'limit': 100
        }

    
    url = 'https://api.marketstack.com/v1/eod'
    
    try:
        print("/n Inside collect ticker info")
        #print(url)
        print(params)
        api_result = requests.get(url, params)
        api_response = api_result.json()
        print(api_response)
        
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
            'market_ticker': {
                'S': "{}_{}_{}".format(ticker_market, ticker_exchange, ticker_symbol)
                },
            'frequence_timestamp': {
                'S': "DAY_{}".format(ticker_date)
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
            'wally_index_14': {
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
    except Exception as e:
        # print("cannot process ticker {}".format(ticker))
        # print(e)
        # print(api_response)
        pass


def prev_work_day(date=None):
    print(date)
    if not date:
        #rise exception
        pass
    print(date)
    date_array = date.replace("DAY_", "").split("T")[0].split("-")
    today = datetime.datetime(int(date_array[0]), int(date_array[1]), int(date_array[2]))
    print(today)
    offset = max(1, (today.weekday() + 6) % 7 - 3)
    timedelta = datetime.timedelta(offset)
    most_recent = today - timedelta
    return most_recent



def data_collector(event, context):
    print("start lambda")
    
    print(event)
    print(event.get("params"))
    # local_event = json.loads(event)
    # print(local_event) 
    # print(context)
    if event.get("params", None):
        if event.get("params").get("dates", None):
            for date in event.get("params").get("dates", None):
                print(date)
            return collect_data(date="2021-03-09")
    return collect_data()


def collect_data(date=None):
    dynamodb = boto3.client('dynamodb')
    result = []
    for ticker in TICKER_LIST:
        try:
            test = collect_ticker_indexes(ticker,date)
            # print(test)
            dynamodb.put_item(TableName='financialData', Item=test)
            result.append(test)
            
        except Exception as e:
            #print("cannot process ticker {}".format(ticker))
            #print(e)
            pass
    
    result.sort(key=lambda x: x['wally_index_14']['N'], reverse=True)
    print('\n------------------------------\n')
    signal_list = []
    for item in result[0:5]:
        print('{} - {} - {}'.format(item['market_ticker'], item['wally_index_14'], item['dix']))
        if float(item['dix']['N']) < 0:
            print("dix negative, no signal today")
            signal_list = []
            # break
        current_date = item['frequence_timestamp']['S']
        previous_working_day = prev_work_day(current_date)
        print(previous_working_day)
        ddb_resource = boto3.resource('dynamodb')
        table = ddb_resource.Table('financialData')

        response = table.query(
            KeyConditionExpression='market_ticker = :m_ticker',
            ExpressionAttributeValues={
                ':m_ticker': item['market_ticker']['S'],
            }
        )
        
        if len(response['Items']) > 1:
            today_wally_14 = float(item['wally_index_14']['N'])
            today_sx = float(item['sx']['N'])
            yesterday_wally_14 = response['Items'][1]['wally_index_14']
            yesterday_sx = response['Items'][1]['sx']
            print("t_w: {}, t_sx: {}, y_w: {}, y_s: {}".format(today_wally_14, today_sx, yesterday_wally_14, yesterday_sx))
            if (yesterday_wally_14 < yesterday_sx) and (today_wally_14 > today_sx):
                # Note the list should be already ordered with the highest wally_14
                print("we have signal")
                signal_list.append(item)
    
    print("\nsignal: {}",format(signal_list))
    if len(signal_list) > 1:

        signal_value = {
            'market_ticker': {
                'S': "SIGNAL",
                },
            'frequence_timestamp': {
                'S': signal_list[0]['frequence_timestamp']['S']
                },
            "signal_list": signal_list
            }
        print(signal_value)
        dynamodb.put_item(TableName='financialData', Item=signal_value)