"""
fuente de referencia:
    Python para Finanzas Quant - APIS - Conexion Mkts Pag 202.
"""
import websocket as ws
import pandas as pd
import json
#Funcion Guardado en DB
from sqlalchemy import create_engine
import db

ultimo_dato={}

'''
https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams

Stream Name: <symbol>@bookTicker

Update Speed: Real-time

{
  "u":400900217,     // order book updateId
  "s":"BNBUSDT",     // symbol
  "b":"25.35190000", // best bid price
  "B":"31.21000000", // best bid qty
  "a":"25.36520000", // best ask price
  "A":"40.66000000"  // best ask qty
}

'''

def wLibroTicker(moneda1='btc',moneda2='usdt',ultimo_dato=ultimo_dato):
    '''
    Suscripcion al libro de un ticker
    '''
    
    #Creamos la conexion.
    wss=f'wss://stream.binance.com:9443/ws/{moneda1}{moneda2}@bookTicker'
    conn=ws.create_connection(wss)
    
    #Creamos el Json con los datos de suscripcion
    subscribe='{"method":"SUBSCRIBE","params":["'+moneda1+moneda2+'@bookTicker"],"id":1}'
    
    #Nos suscreibimos al websocket
    conn.send(subscribe)

    #Recibimos datos
    while True:
        try:
            #Tomo el dato
            data=conn.recv()
            #Lo convierto en diccionario
            dic=eval(data)
            print(dic)
            if(ultimo_dato=={}):
                ultimo_dato={dic['u']:dic}
            else:
                allKeys = ultimo_dato.keys()
                allKeysSorted = sorted(allKeys)
                actual=dic['u']
                if(actual!=allKeysSorted[0]):
                    #print(f'nuevo dato:\n{data}')
                    ultimo_dato.update({dic['u']:dic})
                    #print(f'estado del diccionario:\n{ultimo_dato}')
        except:
            #Existe la posibilidad de que data sea nula
            print('Dato nulo')


#no funcional
def GuardoDB(data,broker='binanceticks'):
    # conexion a la DB
    db_connection = create_engine(db.BD_CONNECTION)
#    conn = db_connection.connect()

    # creo la tabla
    create_table = f'''
        CREATE TABLE IF NOT EXISTS `{broker}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `ticker` varchar(20) DEFAULT '',
          `time` timestamp NULL DEFAULT NULL,
          `tradeid` float(10) DEFAULT NULL,
          `price` float(10) DEFAULT NULL,
          `quantity` float(10) DEFAULT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `idx_ticker_time` (`ticker`,`time`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
        '''

    db_connection.execute(create_table)
       
    
    data.to_sql(con=db_connection, name=broker, if_exists='append')


'''
https://binance-docs.github.io/apidocs/spot/en/#trade-streams

Stream Name: <symbol>@trade

Update Speed: Real-time

{
  "e": "trade",     // Event type
  "E": 123456789,   // Event time
  "s": "BNBBTC",    // Symbol
  "t": 12345,       // Trade ID
  "p": "0.001",     // Price
  "q": "100",       // Quantity
  "b": 88,          // Buyer order ID
  "a": 50,          // Seller order ID
  "T": 123456785,   // Trade time
  "m": true,        // Is the buyer the market maker?
  "M": true         // Ignore
}
Tanto el 'Event time' como el 'Trade time' se repiten, por este motivo utilizo
el 'Trade ID' como clave.
'''


def wtick(moneda1='btc',moneda2='usdt',ultimo_dato=ultimo_dato):
    '''
    Suscripcion al libro de un ticker
    '''
    
    #Creamos la conexion.
    wss=f'wss://stream.binance.com:9443/ws/{moneda1}{moneda2}@trade'
    conn=ws.create_connection(wss)
    
    #Creamos el Json con los datos de suscripcion
    subscribe='{"method":"SUBSCRIBE","params":["'+moneda1+moneda2+'@trade"],"id":1}'
    
    #Nos suscreibimos al websocket
    conn.send(subscribe)
   
    
    #Recibimos datos
    while True:
        try:
            #Tomo el dato
            data=conn.recv()
            data_mod=data.replace(',"m":true,"M":true','')
    
            #Lo convierto en diccionario
            dic=eval(data_mod)
            print(dic)
            if(ultimo_dato=={}):
                ultimo_dato={dic['t']:dic}
            else:
                allKeys = ultimo_dato.keys()
                allKeysSorted = sorted(allKeys)
                actual=dic['t']
                if(actual!=allKeysSorted[0]):
                    #print(f'nuevo dato:\n{data}')
                    ultimo_dato.update({dic['t']:dic})
                    #print(f'estado del diccionario:\n{ultimo_dato}')
        except:
            #Existe la posibilidad de que data sea nula
            #print('Dato nulo')
            pass
            
#Casos de Uso.
if __name__ == '__main__':
    #Uso uno u otro, sino da un error de indices.
    #print('Suscripcion al libro de un ticker')
    #wLibroTicker() 
    
    wtick()

