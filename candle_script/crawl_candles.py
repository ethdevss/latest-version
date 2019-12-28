import requests

from marshmallow import Schema, fields, pprint
import datetime as dt

from mongoengine import *
import pymongo

from time import sleep

class MarketData(Document):
    timestamp = DateTimeField(required=True, unique=True)
    symbol = StringField(required=True)
    open = IntField(required=True)
    high = IntField(required=True)
    low = IntField(required=True)
    close = IntField(required=True)
    trades = IntField(required=True)
    volume = IntField(required=True)
    meta = {'collection': 'candles'}


end_time = dt.datetime.now()
start_time = dt.datetime.now() - dt.timedelta(days=15)

class TimeSchema(Schema):
    end_time = fields.DateTime()
    start_time = fields.DateTime()

schema = TimeSchema()

connection = connect(db='market_data')

while True:
    if start_time > end_time + dt.timedelta(hours=8):
        break

    input_data = {
        'end_time': start_time + dt.timedelta(hours=8),
        'start_time': start_time
    }

    result = schema.dump(input_data)
    startTime = result['start_time']
    endTime = result['end_time']

    dataset_url = "https://www.bitmex.com/api/v1/trade/bucketed?binSize=5m&partial=false&symbol=XBTUSD&reverse=true"
    
    start_param = "&startTime=" + startTime
    end_param = "&endTime=" + endTime

    sleep(2)
    url = dataset_url + start_param + end_param
    response = requests.get(url)
    ohlcv = response.json()

    start_time = start_time + dt.timedelta(hours=8)
   
    for data in ohlcv:
        timestamp = data['timestamp']
        symbol = data['symbol']
        open = data['open']
        high = data['high']
        low = data['low']
        close = data['close']
        trades = data['trades']
        volume = data['volume']
        marketdata = MarketData(timestamp=timestamp, symbol=symbol, open=open,
                                high=high, low=low, close=close, trades=trades,
                                volume=volume)
        try:
            marketdata.save()
        except pymongo.errors.DuplicateKeyError:
            pass
        except NotUniqueError:
            pass
        except Exception as e:
            print(e)
