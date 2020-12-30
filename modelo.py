import numpy as np

import utils

# import matplotlib.pyplot as plt
# import matplotlib as mpl
# import seaborn as sns

from IPython.display import clear_output
import pickle

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix


def entrenar(df):

    X_train, X_test, y_train, y_test = train_test_split(df.iloc[:,1:-1], df.pred, test_size=0.3)

    modelo_rf = RandomForestClassifier(criterion = 'entropy', max_depth=15)
    modelo_rf.fit(X_train, y_train)
    y_pred = modelo_rf.predict(X_test)

    # guardamos la data
    with open('bot_rf.dat', 'wb') as file:
        pickle.dump(modelo_rf,file)

    m = np.array(confusion_matrix(y_test, y_pred, normalize='all')).round(2)
    matriz = {'verdadero_positivo': m[1][1], 'verdadero_negativo': m[0][0], 'falso_positivo': m[0][1],
              'falso_negativo': m[1][0]}
    resumen = {'aciertos': f"{matriz['verdadero_positivo'] + matriz['verdadero_negativo']:.1%}",
               'sesgo +': f"{matriz['falso_positivo'] + matriz['falso_negativo']:.1%}"}

    return print('Bosques Aleatorios: \n', m, '\nPorcentajes:\n', matriz, '\n', resumen)


# levantar el modelo entrenado

def traerModelo(tipo='RF'):
    if tipo == 'RF':
        with open('bot_rf.dat', 'rb') as file:
            modelo = pickle.load(file)
    else:
        modelo = None
        print('no encontre el modelo que pediste')

    return modelo

# PREDECIR
def predecir(data, modelo):
    try:
        actual = utils.agregar_indicadores_predecir(data).iloc[-1]
        y_pred = modelo.predict((actual,))[0]
        y_proba = modelo.predict_proba((actual,))[0]
        return y_pred, y_proba
    except:
        print('No se pudo predecir')
        return None, None