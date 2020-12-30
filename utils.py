from sqlalchemy import create_engine
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import logging
import db
import numpy as np


def GuardoDB(data, ticker, broker='binance'):
    # conexion a la DB
    db_connection = create_engine(db.BD_CONNECTION)
    conn = db_connection.connect()

    # creo la tabla
    create_table = f'''
        CREATE TABLE IF NOT EXISTS `{broker}` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `ticker` varchar(20) DEFAULT '',
          `time` timestamp NULL DEFAULT NULL,
          `open` float(10) DEFAULT NULL,
          `high` float(10) DEFAULT NULL,
          `low` float(10) DEFAULT NULL,
          `close` float(10) DEFAULT NULL,
          `volume` float(10) DEFAULT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `idx_ticker_time` (`ticker`,`time`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
        '''

    db_connection.execute(create_table)

    data.to_sql(con=db_connection, name=broker, if_exists='append')


def dato_historico(moneda1='BTC', moneda2='USDT', timeframe='1m', desde='datetime', hasta='datetime', limit=1000):
    '''desde=datetime.fromisoformat('2020-11-05') #YYYY-MM-DD
        hasta=datetime.fromisoformat('2020-11-09') # No es inclusive.
        data=dato_historico(moneda1='BTC', moneda2='USDT',timeframe='1m',desde=desde,hasta=hasta)'''

    # Creo la variable Symbol
    symbol = moneda1 + moneda2

    # presumo que las fechas son UTC0
    desde = desde.replace(tzinfo=pytz.utc)
    hasta = hasta.replace(tzinfo=pytz.utc) + timedelta(days=1)
    # Llevo las variables Datetime a ms
    startTime = int(desde.timestamp() * 1000)
    endTime = int(hasta.timestamp() * 1000)
    # Inicializo los df
    df_acum = pd.DataFrame(columns=('openTime', 'open', 'high', 'low', 'close', 'volume', 'cTime',
                                    'qVolume', 'trades', 'takerBase', 'takerQuote', 'Ignore'))
    df = pd.DataFrame(columns=('openTime', 'open', 'high', 'low', 'close', 'volume', 'cTime',
                               'qVolume', 'trades', 'takerBase', 'takerQuote', 'Ignore'))

    # dos banderas que uso para saber que termine.
    finished = False
    ultimaFechaAnterior = False

    url = 'https://api.binance.com/api/v3/klines'
    while not finished:

        try:
            ultimaFecha = df.sort_values(by='openTime', ascending=True).iloc[-1]['openTime']
        except:
            ultimaFecha = startTime

        # Inicio Bajada
        params = {'symbol': symbol, 'interval': timeframe,
                  'startTime': ultimaFecha, 'limit': limit}

        # Logueo del Estado pre-Bajada.
        # logging.info(f'Inicio de Bajada Fecha: {ultimaFecha}')
        print(f'Inicio de Bajada Fecha: {ultimaFecha}. Tamaño:{df_acum.size}')

        r = requests.get(url, params=params)
        js = r.json()

        print(js)

        if js == []:
            print('Fechas no Validas')
            finished = True

        # Armo el dataframe
        cols = ['openTime', 'open', 'high', 'low', 'close', 'volume', 'cTime',
                'qVolume', 'trades', 'takerBase', 'takerQuote', 'Ignore']

        if (ultimaFecha >= endTime):
            finished = True
            break

        if (ultimaFecha == ultimaFechaAnterior):
            # Cuando se repite la ultima fecha, el ultimo registro tiene el
            # mismo opentime, pero cambia el volument
            finished = True
            df_acum.drop_duplicates('openTime', inplace=True)
            break

        else:
            ultimaFechaAnterior = ultimaFecha

        df = pd.DataFrame(js, columns=cols)
        df_acum = pd.concat([df_acum, df], join='inner', axis=0)


    # Convierto los valores strings a numeros
    df_acum = df_acum.apply(pd.to_numeric)

    # Elimino columnas que no quiero
    df_acum.drop(['cTime', 'takerBase', 'takerQuote', 'Ignore', 'trades', 'qVolume'], axis=1, inplace=True)

    # Renombro las columnas segun lo acordado.
    df_acum.columns = ['time', 'open', 'high', 'low', 'close', 'volume']

    # Elimino las filas que me trajo extras en caso que existan
    df_acum = df_acum[df_acum.time < endTime]

    # Le mando indice de time
    df_acum.index = pd.to_datetime(df_acum.time, unit='ms')

    # Agrego columna ticker.
    df_acum['ticker'] = moneda1

    # Elimino la columna time, no la clave.
    df_acum.drop(['time'], axis=1, inplace=True)
    df_acum.drop_duplicates(inplace=True)

    return df_acum


def guardado_historico(moneda1='BTC', moneda2='USDT', timeframe='1m', desde='datetime', hasta='datetime',
                       broker='binance'):
    # En caso de no tener fecha de Fin, utilizo el dia actual.
    if hasta == 'datetime':
        hasta = datetime.utcnow()
    # En caso de no tener fecha de Inicio, utilizo 6 meses antes.
    if desde == 'datetime':
        meses = relativedelta(months=6)
        desde = hasta - meses

    try:
        # conexion a la DB
        db_connection = create_engine(db.BD_CONNECTION)
        conn = db_connection.connect()

        # Busco el ultimo dato guardado.
        busquedaUltimaFecha = f'SELECT `id`,`time` FROM {broker} WHERE `ticker` = "{moneda1}" ORDER BY `time` DESC limit 0,1'
        ultimaFecha = db_connection.execute(busquedaUltimaFecha).fetchone()

        # Si encuentro un ultimo registro, lo elimino
        if (ultimaFecha):
            id = ultimaFecha[0]
            query_borrado = f'DELETE FROM {broker} WHERE `id`={id}'
            db_connection.execute(query_borrado)
            desde = ultimaFecha[1]
    except:
        # Dio un error pq no encontro la tabla, la va a crear dentro de la funcion dato_historico
        pass

    # Bajo Informacion.
    data = dato_historico(moneda1=moneda1, moneda2=moneda2, timeframe=timeframe, desde=desde, hasta=hasta)

    # Guardo en DB
    GuardoDB(data, moneda1, broker=broker)

def cruceVol(df,cruces):
    for cruce in cruces:
        clave = 'vol_' + str(cruce[0]) + '_' + str(cruce[1])
        df[clave] = (df.vol_usd.rolling(cruce[0]).mean() / df.vol_usd.rolling(cruce[1]).mean() - 1) * 100
    return df

def cruceEMA(df,cruces):
    for cruce in cruces:
        clave = str(cruce[0]) + '_' + str(cruce[1])
        df[clave] = (df.close.ewm(span=cruce[0]).mean() / df.close.ewm(span=cruce[1]).mean() - 1) * 100
    return df

def rsi(df,ruedas=14):
    df["diferencia"] = df.close.diff()
    df["dif_pos"] = np.where(df["diferencia"] > 0, df["diferencia"], 0)
    df["dif_neg"] = np.where(df["diferencia"] < 0, df["diferencia"], 0)
    df["media_pos"] = df["dif_pos"].ewm(alpha=1 / ruedas).mean()
    df["media_neg"] = df["dif_neg"].ewm(alpha=1 / ruedas).mean()
    df["rs"] = df["media_pos"] / abs(df["media_neg"])
    df["RSI"] = round(100 - 100 / (1 + df["rs"]), 2)
    df = df.drop(["diferencia", "dif_pos", "dif_neg", "media_pos", "media_neg", "rs"], axis=1)
    return df

def agregar_indicadores(df):
    cruces = [(2, 20), (2, 40), (2, 60), (2, 100), (2, 200), (5, 20), (5, 50), (5, 100), (5, 200), (5, 400), (10, 20),
              (10, 50), (10, 100),
              (10, 200), (10, 500), (20, 50), (20, 100), (20, 200), (20, 500), (20, 1000), (50, 100), (50, 200),
              (50, 500), (50, 1000),
              (100, 200), (100, 400), (100, 500), (100, 1000), (200, 500), (200, 1000), (400, 1000)]

    cruces_vol= [(2, 60), (4, 60), (10, 60), (20, 60), (30, 60)]

    # Agrego las medias
    df =cruceEMA(df = df,cruces = cruces)

    # Agrego RSI
    df = rsi(df)

    # Dif volumen
    df['vol_usd'] = (df.open + df.high + df.low + df.close) / 4 * df.volume
    df = cruceVol(df=df, cruces=cruces_vol)

    # Agrego el valor forward de 60 minutos
    df['fw_60'] = (df.close.shift(-60) / df.close - 1) * 100
    df['pred'] = np.where(df.fw_60 > 0, 1, 0)
    df = df.dropna().round(4)

    return df


def agregar_indicadores_predecir(df):
    cruces = [(2, 20), (2, 40), (2, 60), (2, 100), (2, 200), (5, 20), (5, 50), (5, 100), (5, 200), (5, 400), (10, 20),
              (10, 50), (10, 100),
              (10, 200), (10, 500), (20, 50), (20, 100), (20, 200), (20, 500), (20, 1000), (50, 100), (50, 200),
              (50, 500), (50, 1000),
              (100, 200), (100, 400), (100, 500), (100, 1000), (200, 500), (200, 1000), (400, 1000)]

    cruces_vol= [(2, 60), (4, 60), (10, 60), (20, 60), (30, 60)]

    # Agrego las medias
    df =cruceEMA(df = df,cruces = cruces)

    # Agrego RSI
    df = rsi(df)

    # Dif volumen
    df['vol_usd'] = (df.open + df.high + df.low + df.close) / 4 * df.volume
    df = cruceVol(df=df, cruces=cruces_vol)

    # Elimino columnas
    df.drop(['open', 'close', 'high', 'low', 'volume', 'vol_usd'], axis=1, inplace=True)

    df = df.dropna().round(4)

    return df

def elimino_colunas(df):
    df.drop(['open','close','high','low','volume','vol_usd','fw_60'],axis=1,inplace=True)
    return df


