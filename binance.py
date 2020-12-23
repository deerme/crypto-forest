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

# ENVIO DE OPERARION DE PRUEBA
def tradeTest(key,secret):
    endpoint='/api/v3/order/test'
    url=base+endpoint    
    ts=horaservidor()
    params = {'symbol':'BTCUSDT','quantity':5, 'side':'SELL','type':'MARKET','timestamp':ts}
    # signature
    h = urlencode(params)
    b = bytearray()
    b.extend(secret.encode())
    signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

    headers = {'X-MBX-APIKEY': key}
    r = requests.post(url=url, params=params, headers=headers, verify=True)
    return r



def horaservidor():
    endpoint='/api/v3/time'
    url=base+endpoint
    params = {}
    r = requests.get(url, params=params)
    js = r.json()
    return js['serverTime']


#Caso de Uso.
if __name__ == '__main__':
    print('Estado de la cuenta:')
    data_cuenta = account_snapshot(key= keys.BINANCE_KEY, secret=keys.BINANCE_SECRET)
    print(data_cuenta)
    pruebaTrade=tradeTest(key= keys.BINANCE_KEY, secret=keys.BINANCE_SECRET)
    print(pruebaTrade)

