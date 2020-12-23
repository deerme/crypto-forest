"""
Created on Mon Dec  7 10:50:00 2020

@author: nicopacheco121
"""

import datetime as dt
import requests
import hmac
import hashlib
import keys

try:
    from urllib import urlencode
except:
    from urllib.parse import urlencode



""" WALLET ENDPOINTS"""
url = 'https://api.binance.com/'


# SYSTEM STATUS
def system_status():

    url = 'https://api.binance.com/wapi/v3/systemStatus.html'
    r = requests.get(url)
    js= r.json()
    return js


# DAILY ACCOUNT SNAPSHOT (RESUMEN DIARIO DE LA CUENTA)
def account_snapshot(key,secret):
    url ='https://api.binance.com/sapi/v1/accountSnapshot'

    # timestamp (le tuve que sumar 15 segundos, no se por que)
    ts = int(dt.datetime.timestamp(dt.datetime.now() + dt.timedelta(seconds=15)) * 1000)
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

#Caso de Uso.
#print(account_snapshot(keys.BINANCE_KEY,keys.BINANCE_SECRET))


if __name__ == '__main__':
    data_cuenta = account_snapshot(key= keys.BINANCE_KEY, secret=keys.BINANCE_SECRET)
    print(data_cuenta)
