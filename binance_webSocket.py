"""
fuente de referencia:
    Python para Finanzas Quant - APIS - Conexion Mkts Pag 202.
"""
import websocket as ws
import pandas as pd
import json

def wLibroTicker(moneda1='btc',moneda2='usdt'):
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
    for i in range(5):
        data=conn.recv()
        print(data)    

def wPuntas(moneda1='btc',moneda2='usdt',depth='5/10/20'):
    '''Suscripcion al libro de puntas'''
   #Creamos la conexion.
    wss=f'wss://stream.binance.com:9443/ws/{moneda1}{moneda2}@depth{depth}@100ms'
    conn=ws.create_connection(wss)
    
    #Creamos el Json con los datos de suscripcion
    subscribe='{"method":"SUBSCRIBE","params":["'+moneda1+moneda2+'@depth'+str(depth)+'@100ms"],"id":10}'
    
    #Nos suscreibimos al websocket
    conn.send(subscribe)
    
    #Recibimos datos
    conn.recv()
    
    for i in range(4):
        data=conn.recv()
        df=pd.DataFrame(json.loads(data))
        print(df,'\n')    
    
#Casos de Uso.
if __name__ == '__main__':
    #Uso uno u otro, sino da un error de indices.
    #print('Suscripcion al libro de un ticker')
    #wLibroTicker() 
    
    print('Suscripcion al libro de puntas')
    wPuntas(depth=5)
