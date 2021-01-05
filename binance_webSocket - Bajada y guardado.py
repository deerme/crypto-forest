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
# Hilos
import threading
import time


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

def wLibroTicker(moneda1='btc',moneda2='usdt'):
    '''
    Suscripcion al libro de un ticker
    '''
    #Creo la variable global
    global ultimo_dato
    ultimo_dato={}
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
          `tradeid` int(11) NULL DEFAULT NULL,
          `price` float(10) DEFAULT NULL,
          `quantity` float(10) DEFAULT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `idx_ticker_tradeid` (`ticker`,`tradeid`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
        '''

    db_connection.execute(create_table)
       
    
    data.to_sql(con=db_connection, name=broker, if_exists='append',index=False)


            
def guardo():
    global ultimo_dato
    while True:
        #cada 5 minutos guardo info.
        time.sleep(10)
    
        
        if (ultimo_dato!={}):
            #copio el diccionario, esto me permite seguir sumando datos a ultimo_dato
            ultima_base=ultimo_dato.copy()
        
            #Preparo el DF, desde el diccionario.
            df = pd.DataFrame([key for key in ultima_base.keys()], columns=['tradeid'])
            df['ticker']=[value['s'] if 's' in value.keys() else None for value in ultima_base.values()]
            df['price']=[value['p'] if 'p' in value.keys() else None for value in ultima_base.values()]
            df['quantity']=[value['q'] if 'q' in value.keys() else None for value in ultima_base.values()]
            df['time_ms']=[value['T'] if 'T' in value.keys() else None for value in ultima_base.values()]
            #convierto la fecha
            df['time'] = pd.to_datetime(df.time_ms, unit='ms')
            # Elimino columnas que no quiero
            df.drop(['time_ms'],axis=1,inplace=True)
            #Guardo en la base de datos.
            GuardoDB(data=df)
            #Elimino en ultimo_dato, los tradeid ya guardados.
            for key in ultima_base.keys(): del ultimo_dato[key]


        
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


def wtick(moneda1='btc',moneda2='usdt'):
    '''
    Suscripcion al libro de un ticker
    '''
    #Creo la variable global
    global ultimo_dato
    ultimo_dato={}
   
    #Creamos la conexion.
    wss=f'wss://stream.binance.com:9443/ws/{moneda1}{moneda2}@trade'
    conn=ws.create_connection(wss)
    
    #Creamos el Json con los datos de suscripcion
    subscribe='{"method":"SUBSCRIBE","params":["'+moneda1+moneda2+'@trade"],"id":1}'
    
    #Nos suscreibimos al websocket
    conn.send(subscribe)
   
    
    #Recibimos datos
    while True:
#    contador=0
#    while contador<5:
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
#        contador+=1
            
#Casos de Uso.
if __name__ == '__main__':
    #Uso uno u otro, sino da un error de indices.
    #print('Suscripcion al libro de un ticker')
    #wLibroTicker() 
#    wtick()
#    guardo()
    

    hilo1 = threading.Thread(target=wtick)
    hilo2 = threading.Thread(target=guardo) 
    hilo1.start()
    hilo2.start()
