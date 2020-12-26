"""
Created on Mon Dec  7 10:50:00 2020

@author: nicopacheco121

fuente de referencia:
    https://binance-docs.github.io/apidocs/spot/en/#introduction
"""

import requests
import hmac
import hashlib
import keys
import time

try:
    from urllib import urlencode
except:
    from urllib.parse import urlencode



""" WALLET ENDPOINTS"""
base = 'https://api.binance.com'


# SYSTEM STATUS
def system_status():

    url = 'https://api.binance.com/wapi/v3/systemStatus.html'
    r = requests.get(url)
    js= r.json()
    return js


# DAILY ACCOUNT SNAPSHOT (RESUMEN DIARIO DE LA CUENTA)
    
def account_snapshot(key,secret):
    endpoint='/sapi/v1/accountSnapshot'
    url=base+endpoint 
    ts=horaservidor()
    recvWindow = 5000

    params = {'recvWindow': 10000, "type": 'SPOT', 'timestamp': ts}

    # signature
    h = urlencode(params)
    b = bytearray()
    b.extend(secret.encode())
    signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {'X-MBX-APIKEY': key}
    r = requests.get(url=url, params=params, headers=headers, verify=True)
    js = r.json()

    return js

# Current Open Orders (USER_DATA)

def current_open_Orders(key,secret):
    endpoint='/api/v3/openOrders'
    url=base+endpoint 
    ts=horaservidor()

    params = {'timestamp': ts}

    # signature
    h = urlencode(params)
    b = bytearray()
    b.extend(secret.encode())
    signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {'X-MBX-APIKEY': key}
    r = requests.get(url=url, params=params, headers=headers, verify=True)
    js = r.json()

    return js

# Test New Order (TRADE) (ENVIO DE OPERARION DE PRUEBA)

def tradeTest(key,secret,moneda1='BTC',moneda2='USDT',side='SELL/BUY',
              tipo='LIMIT/MARKET',timeinforce='GTC/IOC/FOK',quantity='NUM',price='NUM'):
    '''
    Type	              Additional mandatory parameters
    LIMIT	              timeInForce, quantity, price
    MARKET	              quantity or quoteOrderQty
    STOP_LOSS	          quantity, stopPrice
    STOP_LOSS_LIMIT	      timeInForce, quantity, price, stopPrice
    TAKE_PROFIT	          quantity, stopPrice
    TAKE_PROFIT_LIMIT	  timeInForce, quantity, price, stopPrice
    LIMIT_MAKER	          quantity, price
    
    -------------------------------------------------------------------------------
    Time in force (timeInForce):
    
    This sets how long an order will be active before expiration.
    
    Status	Description
    GTC	    Good Til Canceled
            An order will be on the book unless the order is canceled.
    IOC	    Immediate Or Cancel
            An order will try to fill the order as much as it can before the order expires.
    FOK	    Fill or Kill
            An order will expire if the full order cannot be filled upon execution.
    '''

    endpoint='/api/v3/order/test'
    # para tradear
    # endpoint='/api/v3/order'
    url=base+endpoint    
    symbol=moneda1+moneda2
    ts=horaservidor()

    if (tipo=='LIMIT'):
        params = {'symbol': symbol,'timestamp':ts,'side':side,'type':tipo,
                  'timeInForce':timeinforce,'quantity':quantity,'price':price}
    if (tipo=='MARKET'):
        params = {'symbol': symbol,'timestamp':ts,'side':side,'type':tipo,
                  'quantity':quantity}
    
    
    # signature
    h = urlencode(params)
    b = bytearray()
    b.extend(secret.encode())
    signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {'X-MBX-APIKEY': key}
    r = requests.post(url=url, params=params, headers=headers, verify=True)
    return r

def dato_actual(moneda1='BTC', moneda2='USDT'):
    '''data=dato_actual(moneda1='BTC', moneda2='USDT')'''
    endpoint='/api/v3/depth'
    url=base+endpoint    
    #Creo la variable Symbol
    symbol=moneda1+moneda2

    params = {'symbol':symbol,'limit':5}
    try:
        r = requests.get(url, params=params)
        js = r.json()
        ask_PAR=js.get('asks')[0][0]   
        bid_PAR=js.get('bids')[0][0]
        return (ask_PAR,bid_PAR)
    except:
        print(f'Error al intentar bajar los datos del simbolo {moneda1}-{moneda2}')
        return (0,0)

def horaservidor():
    endpoint='/api/v3/time'
    url=base+endpoint
    params = {}
    r = requests.get(url, params=params)
    js = r.json()
    return js['serverTime']


#Casos de Uso.
if __name__ == '__main__':
    print('Estado de la cuenta:')
    data_cuenta = account_snapshot(key= keys.BINANCE_KEY, secret=keys.BINANCE_SECRET)
    print(data_cuenta)
    print('Ejemplo de Compra a precio de Mercado')
    pruebaTradeMarket=tradeTest(key= keys.BINANCE_KEY, secret=keys.BINANCE_SECRET,
                          moneda1='BTC',moneda2='USDT',side='SELL',
                          tipo='MARKET',quantity=0.1)
    
    print(pruebaTradeMarket)
    time.sleep(2)
    
    print('Ejemplo de Compra a precio Limite')
    ask,bid=dato_actual()
    pruebaTradeLimit=tradeTest(key= keys.BINANCE_KEY, secret=keys.BINANCE_SECRET,
                          moneda1='BTC',moneda2='USDT',side='SELL',
                          tipo='LIMIT',timeinforce='GTC',quantity=0.1,price=ask)
    print(pruebaTradeLimit)
    time.sleep(2)

    print('Ejemplo de Ordenes abiertas')
    ordenesabiertas=current_open_Orders(key= keys.BINANCE_KEY, secret=keys.BINANCE_SECRET)
    print(ordenesabiertas)
    
    
