# PRECISION = "s"
# MEASUREMENT = "candlesticks"
# EXCHANGE = "bitmex"
#
# SUBSCRIPTION_DICT = {"op":"subscribe", "args":'tradeBin1m:{}'}
# PAIR_URL = "https://www.bitmex.com/api/v1/instrument/active"
# WEBSOCK_URL = "wss://www.bitmex.com/realtime?subscribe=tradeBin5m:XBTUSD"
# TYPE = "PERPEPTUAL"

import websocket
import json
from mongoengine import *

import pymongo
import telegram

bot = telegram.Bot(token='985728867:AAE9kltQqpmIdwPi510h4fzfQas59besQzE')
chat_id = bot.get_updates()[-1].message.chat_id

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


def on_message(ws, message):
    data = dict()
    if "partial" in message:
        message = json.loads(message)
        timestamp = message['data'][0]['timestamp']
        symbol = message['data'][0]['symbol']
        open = message['data'][0]['open']
        high = message['data'][0]['high']
        low = message['data'][0]['low']
        close = message['data'][0]['close']
        trades = message['data'][0]['trades']
        volume = message['data'][0]['volume']
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
    elif "insert" in message:
        message = json.loads(message)
        timestamp = message['data'][0]['timestamp']
        symbol = message['data'][0]['symbol']
        open = message['data'][0]['open']
        high = message['data'][0]['high']
        low = message['data'][0]['low']
        close = message['data'][0]['close']
        trades = message['data'][0]['trades']
        volume = message['data'][0]['volume']
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
            
    print(message)

def on_error(ws, error):
    message = str(error) + "candle collector의 websocket 연결이 종료되었습니다."
    bot.send_message(chat_id=chat_id, text=message)
    ws.on_close(ws)

def on_close(ws):
    print("### closed ###")
    message = "candle collector의 websocket 연결이 종료되었습니다."
    bot.send_message(chat_id=chat_id, text=message)
    ws.close()

def on_open(ws):
    print("### open ###")

def run(endpoint):
    connection = connect(db='market_data')
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(endpoint,
                                on_open = on_open,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)

    while True:
        try:
            ws.run_forever(ping_interval=30, ping_timeout=25)
        except:
            pass

        
if __name__ == "__main__":
    command = 'subscribe=tradeBin5m:XBTUSD'
    endpoint = 'wss://www.bitmex.com/realtime?'+command
    
    run(endpoint)
