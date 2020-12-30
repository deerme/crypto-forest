import utils
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine
import db
import pandas as pd
import pickle
import modelo


pd.options.display.max_columns=50

"""   / / / CONEXION DB / / /   """
sql_engine = create_engine(db.BD_CONNECTION)
sql_conn = sql_engine.connect()

"""   / / / BAJAR Y GUARDAR LA DATA / / /   """
# btc 2 años aprox 20 minutos
#utils.guardado_historico(moneda1 = 'BCH',desde=datetime.utcnow() - timedelta(days=730),broker='binance_bch')
utils.guardado_historico(moneda1 = 'BTC',desde=datetime.utcnow() - timedelta(days=730),broker='binance_btc')



"""   / / / TRAIGO DATA Y AGREGO INDICADORES / / /   """

nombreTabla = 'binance_btc'
q = f"""SELECT * FROM `{nombreTabla}` """
# data = pd.read_sql(q,con=sql_conn)

# GUARDO LA DATA EN PICKLE
# with open('btc_minutes.dat', 'wb') as file:
#      pickle.dump(data,file)

#LEVANTO LA DATA
# with open('btc_minutes.dat', 'rb') as file:
#     data = pickle.load(file)
#
# data.drop(['id'],axis=1,inplace=True)
# data.set_index('time',inplace=True)
#
# start_time = time.time()
# data_con_indicadores = utils.agregar_indicadores(data)
# data_con_indicadores = utils.elimino_colunas(data_con_indicadores)
#
# print(data_con_indicadores)
# print("--- %s seconds ---" % (time.time() - start_time))
#
#
# with open('btc_minutes_indicadores.dat', 'wb') as file:
#      pickle.dump(data_con_indicadores,file)
#
# with open('btc_minutes_indicadores.dat', 'rb') as file:
#     data = pickle.load(file)

# start_time = time.time()
# modelo_entrenado = modelo.entrenar(data)
# print("--- %s seconds ---" % (time.time() - start_time))

# hola = modelo.traerModelo('RF')
# print(hola)






