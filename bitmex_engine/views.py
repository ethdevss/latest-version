from django.shortcuts import render
from django.http import HttpResponse
import requests

from marshmallow import Schema, fields, pprint

import datetime as dt
import numpy as np
import pandas as pd
import hashlib
import hmac
import json
import telegram
import time

from urllib.parse import urlparse, quote, urlencode
import urllib.parse

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler

from mongoengine import *

trading_scheduler = None
authorization_key = "qwboOcOHX2HDMaKY0iZf"
bot = telegram.Bot(token='985728867:AAE9kltQqpmIdwPi510h4fzfQas59besQzE')
chat_id = bot.get_updates()[-1].message.chat_id

chat_sebin = "415000369"
chat_kei = "784845620"

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


class TimeSchema(Schema):
    start_time = fields.DateTime()
    end_time = fields.DateTime()


class TradingScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.bitmex_host = "https://www.bitmex.com"
        self.testnet_host = "https://testnet.bitmex.com"
        self.scheduler.start()
        self.job_id = ''
        self.basis_candle = dict()
        self.trade_candle = dict()
        self.basis_result = False
        self.trade_result = False
        self.target_rsi = 50
        self.order_quantity = 1000
	
    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.scheduler.shutdown()

    def kill_scheduler(self, job_id):
        try:
            self.scheduler.remove_job(job_id)
        except JobLookupError as err:
            print("fail to stop scheduler")
            return

    def create_df(self, close_list):
        df = pd.DataFrame(close_list, columns=['close'])
        rsi_period = 14
        chg = df['close'].diff(1)
        
        gain = chg.mask(chg<0,0)
        df['gain'] = gain

        loss = chg.mask(chg>0,0)
        df['loss'] = loss

        avg_gain = gain.ewm(com=rsi_period-1, min_periods=rsi_period).mean()
        avg_loss = loss.ewm(com=rsi_period-1, min_periods=rsi_period).mean()

        df['avg_gain'] = avg_gain
        df['avg_loss'] = avg_loss

        rs = abs(avg_gain / avg_loss)
        rsi = 100 - (100/(1+rs))

        df['rsi'] = rsi
        return df
    
    def create_low_df(self, low_list):
        df = pd.DataFrame(low_list, columns=['low'])
        return df

    def hello(self, type, job_id):
        print("%s Scheduler process_id[%s] : %d" % (type, job_id, time.localtime().tm_sec))
        start_time = time.time()

        # 현재 시간의 분과 초를 구한다.
        current_timestamp = dt.datetime.now()
        minute = current_timestamp.minute
        second = current_timestamp.second

        # 현재 포지션을 확인한다.
        is_open = self.get_current_position("XBTUSD")

        # 최근 발생한 거래정보를 기반으로 최신 가격정보를 구한다.
        url = "https://www.bitmex.com/api/v1/trade?symbol=XBT&count=10&reverse=true"
        response = requests.get(url)
        last_traded_price = response.json()[0]['price']

        # 포지션을 보유하고있다면, 기준봉, 거래봉을 구하는 로직을 실행하지 않는다.
        # 현재 포지션에 대한 익절 또는 손절을 해야하는지 판단한다.
        if is_open:
            # 포지션 진입 후에 15분이 지났다면, 시장가로 매수 포지션 정리
            # 현재 보유중인 포지션의 수량을 구해서, 해당 수량만큼 반대방향으로 주문을 제출하는 방식으로 개선 가능
            if self.trade_candle['timestamp'] + dt.timedelta(minutes=15) < current_timestamp:
                sell_result = self.marketprice_order("XBTUSD", "Sell", self.order_quantity)
                message = "포지션 진입 후 15분이 지나서 포지션을 정리합니다!" + str(sell_result)
                bot.send_message(chat_id=chat_kei, text=message)
                bot.send_message(chat_id=chat_sebin, text=message) 
            # 15분 전에 포지션을 정리하는 경우는 다음 아래와 같다.
            # 포지션을 정리하기까지 만들어지는 2개의 5분봉 Close 가격이 진입시 가격보다 낮을경우 손절한다.
            elif self.is_almost_candle_close(minute, second):
                if self.trade_candle['price'] > last_traded_price:
                    sell_result = self.marketprice_order("XBTUSD", "Sell", self.order_quantity)
                    message = "포지션 진입 후 만들어진 5분봉의 Close 가격이 진입시 가격보다 낮으므로 손절한다." + str(sell_result)
                    bot.send_message(chat_id=chat_kei, text=message)
                    bot.send_message(chat_id=chat_sebin, text=message)
            return
        # 기준봉, 거래봉에 대한 정보가 있는데 포지션을 보유하고 있는 상황이 아니라면
        # 포지션을 진입했다가, 포지션을 정리한 상황으로 파악할 수 있다.
        # 따라서 새로운 기준봉, 거래봉을 구하게끔 기존의 기준봉, 거래봉을 초기화시킨다.
        elif is_open == False and self.basis_candle and self.trade_candle:
            self.basis_candle = dict()
            self.trade_candle = dict()
            self.basis_result = False
            self.trade_result = False

        # RSI 계산을 위한 캔들 데이터를 가져온다.
        # db.candles.find().sort({'timestamp': -1}).limit(4032) # 14days
        candles = MarketData.objects().order_by('-timestamp')[:4032]

        # 캔들 데이터중 Close가격에 대한 정보를 리스트에 저장한다.
        close_list = [float(candle.close) for candle in candles]
        close_array = [float(candle.close) for candle in candles]

        # 캔들 데이터중 Low가격에 대한 정보를 리스트에 저장한다.
        low_list = [float(candle.low) for candle in candles]
        low_array = [float(candle.low) for candle in candles]
        # numpy array 형식으로 type을 변경한다.
        close_list = np.asarray(close_list)
        close_list = close_list[::-1]

        low_list = np.asarray(low_list)
        low_list = low_list[::-1]

        # historical close 가격을 기반으로 RSI를 구한다.
        # dataframe에는 과거 RSI부터 최신 RSI에 대한 정보가 저장된다.
        df = self.create_df(close_list)
        low_df = self.create_low_df(low_list)
        print(df)
        print(low_df) 
        # 5분봉 마감 직전 case(ex) 4분 50초 ~ 59초, 9분 50초 ~ 59초 등)
        # 기준봉과 거래봉이 둘다 구해지지 않은 경우 : 기준봉을 구하는 로직 실행
        # 기준봉은 구해졌고, 거래봉이 구해지지 않은 경우 : 현재 기준봉을 유지할 수 있는 상황인지 판단, 그리고 거래봉을 구한다.
        # => 현재 기준봉을 유지할 수 있다면, 거래봉을 구한다. 거래봉이 정해지는 순간에 시장가로 매수 포지션에 진입한다.
        # => 현재 기준봉을 유지할 수 없다면, 새로운 기준봉을 구하고, 거래봉을 구하는 로직은 실행하지 않는다.
        # => 거래봉과 기준봉을 전부 구했다면, 기존의 거래봉, 기준봉 정보는 초기화해야 한다.

        if self.is_almost_candle_close(minute, second):
            low_price = self.get_candle_low_price()
            bot.send_message(chat_id=chat_kei, text="5분봉 마감 직전")
            bot.send_message(chat_id=chat_sebin, text="5분봉 마감 직전")
            # 기준봉이 구해져있는 상황이 아니라면 기준봉만 구한다.
            if not self.basis_candle:
                self.basis_result = self.get_basis_candle(low_df, last_traded_price, close_array, current_timestamp, low_price)
            # 기준봉이 구해져있는 상황이라면 거래봉만 구한다.
            else:
                # 기준봉이 구해졌지만, 300분동안 거래봉을 구하지 못한 경우 기준봉은 다시 초기화된다.
                if self.basis_candle['timestamp'] + dt.timedelta(hours=5) < current_timestamp:
                    self.basis_candle = {}
                elif self.is_keep_basis_candle(df, low_df, last_traded_price, close_array, current_timestamp, low_price): # 기준봉을 유지할 수 있는 상황이라면,
                    self.trade_result = self.get_trade_candle(df, last_traded_price, close_array, current_timestamp, low_price) # 거래봉을 구한다.
                else: # 기준봉을 유지할 수 없는 상황이라면,
                    self.trade_result = False # 거래봉 결과를 False로 대입한다.
        
        # 기준봉과 거래봉 둘다 구해진 상황이라면, 바로 시장가 매수 때린다.
        # 포지션 진입 후, 진입시 가격보다 1% 이상 가격이 하락했을경우, 역지정 시장가를 통해 손절한다.
        # 즉 시장가 매수 후 역지정 시장가 주문을 제출해야 한다.
        if self.basis_candle and self.trade_candle:
            buy_result = self.marketprice_order("XBTUSD", "Buy", self.order_quantity)
        execution_time = time.time() - start_time
        #bot.send_message(chat_id=chat_id, text=str(execution_time)) 

    # 기준봉을 성공적으로 구한경우 True를 반환
    # 기준봉을 구하지 못한 경우 False를 반환
    # 기준봉이 구해진 상태에서는 해당 get_basis_candle 함수는 실행하지 않는다.
    def get_basis_candle(self, low_df, current_price, close_array, current_timestamp, low_price):
        # df : created by 5 minute historical candles
        # close_array : 5 minute historical price list

        # get current rsi by current_price
        # 가장 첫번째 element 가격 정보를 제거한다.
        close_array.pop(0)
 
        # 현재 가격을 close array에 추가한다.
        close_array.append(current_price)
        close_array = np.asarray(close_array)
        close_array = close_array[::-1]

        # 현재 가격이 적용된 close_array를 기반으로 새로운 data frame을 구성한다.
        current_df = self.create_df(close_array)

        last_row = low_df.tail(1)

        current_last_row = current_df.tail(1)
        
        before_candle_price = last_row['low']
     
        # 아래 조건을 만족할 경우, basis_candle에는 기준봉 정보가 할당된다. 
        # 조건 : 5붕봉 마감전에 직전 5분봉과의 가격을 비교, 가격이 하락한 경우 RSI가 23미만인지 비교
        before_candle_price = before_candle_price.values[0]

        current_rsi = current_last_row['rsi'].values[0]

        message = "직전 5분봉 low price: " + str(before_candle_price) + '\n' + "현재 가격: " + str(current_price) + '\n' + "현재 RSI : " + str(current_rsi) + '\n' + "현재 만들어지고 있는 봉의 low_price: " + str(low_price)
        bot.send_message(chat_id=chat_kei, text=message)
        bot.send_message(chat_id=chat_sebin, text=message) 
        if before_candle_price > low_price and current_rsi < self.target_rsi:
            self.basis_candle['price'] = low_price
            self.basis_candle['rsi'] = current_rsi
            self.basis_candle['timestamp'] = current_timestamp
            message = "기준봉을 구했습니다. 기준봉 low_price: " + str(low_price) + '\n' + " 기준봉 rsi: " + str(current_rsi) + '\n' + " 기준봉 timestamp: " + str(current_timestamp)
            bot.send_message(chat_id=chat_kei, text=message)
            bot.send_message(chat_id=chat_sebin, text=message)
            return True
        return False


    def is_almost_candle_close(self, minute, second):
        if minute == 4 and (second >= 57 and second <= 59):
            return True
        elif minute == 9 and (second >= 57 and second <= 59):
            return True
        elif minute == 14 and (second >= 57 and second <= 59):
            return True
        elif minute == 24 and (second >= 57 and second <= 59):
            return True
        elif minute == 29 and (second >= 57 and second <= 59):
            return True
        elif minute == 34 and (second >= 57 and second <= 59):
            return True
        elif minute == 39 and (second >= 57 and second <= 59):
            return True
        elif minute == 44 and (second >= 57 and second <= 59):
            return True
        elif minute == 49 and (second >= 57 and second <= 59):
            return True
        elif minute == 54 and (second >= 57 and second <= 59):
            return True
        elif minute == 59 and (second >= 57 and second <= 59):
            return True
        else:
            return False

   
    # 기준봉이 구해진 상황에서, 해당 기준봉을 초기화시켜야 하는 상황인지 판단하는 로직 
    def is_keep_basis_candle(self, df, low_df, current_price, close_array, current_timestamp, low_price):
        # df : created by 5 minute historical candles
        # close_array : 5 minutes historical price list

        close_array.pop(0)
   
        close_array.append(current_price)
        close_array = np.asarray(close_array)
        close_array = close_array[::-1]

        current_df = self.create_df(close_array)

        last_row = df.tail(1)
        current_last_row = current_df.tail(1)
        before_candle_price = last_row['close']
        before_candle_price = before_candle_price.values[0]
    
        before_rsi = last_row['rsi'].values[0] 
        current_rsi = current_last_row['rsi'].values[0]

        last_low_price = low_df.tail(1)
        before_candle_low_price = last_low_price['low']
        before_candle_low_price = before_candle_low_price.values[0]

        # RSI는 하락하였는데, 가격은 하락하지 않은 경우 CASE
        if before_rsi > current_rsi and before_candle_low_price <= low_price:
            self.basis_candle = {}
            message="RSI는 하락했지만, LOW Price는 하락하지 않아서 기준봉이 초기화되었습니다"
            bot.send_message(chat_id=chat_kei, text=message)
            bot.send_message(chat_id=chat_sebin, text=message) 
            return False
        elif before_rsi > current_rsi and before_candle_low_price > low_price:
            # RSI도 하락하였고, 가격도 하락한경우 새로운 기준봉이 생긴다.
            self.basis_candle['price'] = low_price
            self.basis_candle['rsi'] = current_rsi
            self.basis_candle['timestamp'] = current_timestamp
            message="RSI 하락, LOW Price도 하락했으므로 새로운 기준봉이 생겼습니다."
            bot.send_message(chat_id=chat_kei, text=message)
            bot.send_message(chat_id=chat_sebin, text=message)
            return False
        elif current_rsi > self.target_rsi and before_candle_low_price > low_price:
            # RSI 23 초과한 경우 + 기준봉보다 가격이 하락한 CASE
            message="RSI가 target rsi를 초과했고, 기준봉보다 Low price가 하락했으므로 기준봉이 초기화 되었습니다." 
            self.basis_candle = {}
            return False
        return True

    def get_candle_low_price(self):
        recent_trades_url = "https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&reverse=true"
        current_time = dt.datetime.now()
        current_minute = current_time.minute
        current_second = current_time.second
        
        start_time = current_time - dt.timedelta(minutes=4, seconds=current_second) 
          
        input_data = {
            'start_time': start_time,
            'end_time': current_time
        }

        schema = TimeSchema()
        result = schema.dump(input_data)
        startTime = result['start_time']
        endTime = result['end_time']

        start_param = "&startTime=" + startTime
        end_param = "&endTime=" + endTime

        url = recent_trades_url + start_param + end_param
        response = requests.get(url)
        recent_trades = response.json()

        low_price = recent_trades[0]['price']
        for recent_trade in recent_trades:
            if low_price > recent_trade['price']:
                low_price = recent_trade['price']

        return low_price


    # 거래봉은 기준봉과 300분 내에 존재해야 한다. 
    def get_trade_candle(self, df, current_price, close_array, current_timestamp, low_price):
        # 거래봉이 되기 위해서는 기준봉보다 rsi가 높아야한다.
        # 거래봉은 기준봉보다 가격이 하락했을때만 만들어진다.
        # df : created by 5 minute historical candles
        # close_array : 5 minute historical price list

        # get current rsi by current_price
        # 가장 첫번째 element 가격 정보를 제거한다.
        close_array.pop(0)
 
        # 현재 가격을 close array에 추가한다.
        close_array.append(current_price)
        close_array = np.asarray(close_array)
        close_array = close_array[::-1]

        # 현재 가격이 적용된 close_array를 기반으로 새로운 data frame을 구성한다.
        current_df = self.create_df(close_array)

        last_row = df.tail(1)
        current_last_row = current_df.tail(1)
        before_candle_price = last_row['close']

        before_rsi = last_row['rsi'].values[0]
        current_rsi = current_last_row['rsi'].values[0]

        # 기준봉보다 아래서 거래가 됐을경우를 판단
        # 현재 시간이 18시 14분 57초라면, 우선 분은 항상 4분을 마이너스한다. 초는 현재시간을 마이너스한다.
        # 결과적으로 18시 10분 00초 ~ 18시 14분 57초때에 trade 목록을 구할 수 있다.
        # 구한 가격 목록 중, 가장 낮은 가격정보를 찾아서, 기준봉보다 아래서 거래가 된 가격인지 판단한다.
           
        if self.basis_candle['price'] > low_price and current_rsi < self.target_rsi and current_rsi > self.basis_candle['rsi']:
            self.trade_candle['price'] = low_price
            self.trade_candle['rsi'] = current_rsi
            self.trade_candle['timestamp'] = current_timestamp
            message = "거래봉을 구했습니다! " + "거래봉 Low price: " + str(low_price) + '\n' + " 거래봉 rsi: " + str(current_rsi) + '\n' + " 거래봉 timestamp: " + str(current_timestamp) + '\n' + " 포지션 진입시 가격: " + str(current_price)
            bot.send_message(chat_id=chat_kei, text=message)
            bot.send_message(chat_id=chat_sebin, text=message)
            return True
        else:
            return False
 
    
    # Add the proper headers via the `expires` scheme.
    def get_private_request_header(self, verb, path, body):
        expires = int(time.time() + 3600)
        post_body = body
        if body == '':
            post_body = ''
        signature = self.generate_signature(self.api_secret, verb, path, expires, post_body)
        headers = {
            'content-type': 'application/json',
            'Accept': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'api-expires': str(expires),
            'api-key': self.api_key,
            'api-signature': signature
        }
        return headers

    def generate_signature(self, secret, verb, path, expires, postBody):
        message = bytes(verb + path + str(expires) + postBody, 'utf-8')
        signature = hmac.new(bytes(secret, 'utf-8'), message, digestmod=hashlib.sha256).hexdigest()
        return signature 

    
    # 현재 포지션을 보유하고 있는지 확인
    def get_current_position(self, symbol):
        host = self.bitmex_host
        filter = '{"symbol": "XBTUSD"}'
        filter = urllib.parse.quote_plus(filter)
        path = '/api/v1/position' + '?filter=' + filter
        url = host + path
        headers = self.get_private_request_header("GET", path, '')
        r = requests.get(url, headers=headers)
        result_json = r.json()
        if not result_json:
            return False
        else:
            is_open = result_json[0]['isOpen']
            return is_open


    # 시장가로 매수한다. 시장가로 매수하므로 포지션이 생기게된다.
    # 포지션을 보유하고 있는 경우에는 기준봉과 거래봉을 구하지 않는다.
    def marketprice_order(self, symbol, side, quantity):
        host = self.bitmex_host
        ordType = "Market"
        orderQty = quantity
        postBody = {"symbol": symbol, "side": side, "orderQty": quantity, "ordType": "Market"}
        postBody = json.dumps(postBody)
        path = '/api/v1/order'
        url = host + path
        headers = self.get_private_request_header("POST", path, postBody)
        r = requests.post(url, headers=headers, data=postBody)
        result_json = r.json()

         
    def add_scheduler(self, type, job_id):
        if type == 'interval':
            self.scheduler.add_job(self.hello, type, seconds=3, id=job_id, args=(type, job_id))


def index(request):
    return HttpResponse("Hello, world. You're at the polls index.")


def add_strategy(request):
    if request.headers['Authorization'] == authorization_key:
        global trading_scheduler
        trading_scheduler.add_scheduler('interval', "2")
        return HttpResponse("run strategy")
    else:
        return HttpResponse("Invalid Request")


def remove_strategy(request):
    if request.headers['Authorization'] == authorization_key:
        global trading_scheduler
        trading_scheduler.kill_scheduler("2")
        return HttpResponse("stop strategy")
    else:
        return HttpResponse("Invalid Request")


def init_engine(request):
    if request.headers['Authorization'] == authorization_key:
        global trading_scheduler
        trading_scheduler = TradingScheduler()
        # connect to mongo db
        connection = connect(db='market_data')
        return HttpResponse("Initialize Trading Engine")
    else:
        return HttpResponse('Invalid Request')


def shutdown_engine(request):
    if request.headers['Authorization'] == authorization_key:
        global trading_scheduler
        del trading_scheduler
        return HttpResponse("Shutdown Trading Engine")
    else:
        return HttpResponse('Invalid Request')
