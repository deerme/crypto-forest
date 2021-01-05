# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 08:28:29 2021

@author: ltaboada
prueba para ser usada en el archivo de hilos.
"""

import threading



def contar():
    '''Contar hasta cien'''
    global contador
    global dic
    contador=0
    dic={}
    while contador<100:
        contador+=1
        dic2={f'{contador}':threading.current_thread().ident}
        dic.update(dic2)
        print('Hilo:', 
              threading.current_thread().getName(), 
              'con identificador:', 
              threading.current_thread().ident,
              '\nContador:', dic)
def contarb():
    '''Contar hasta cien'''
    global contador
    global dic
    while contador<100:
        contador+=1
        dic2={f'{contador}':threading.current_thread().ident}
        dic.update(dic2)
        print('Hilo:', 
              threading.current_thread().getName(), 
              'con identificador:', 
              threading.current_thread().ident,
              '\nContador:', dic)
def saca():
    global contador
    global dic
    while contador<100:
        if(len(dic)>10):
            dicborrar=dic.copy()
            print(f'\n--------------------Borro dic--------------------\n{dicborrar.keys()}')
            for key in dicborrar.keys(): del dic[key]
                                

hilo1 = threading.Thread(target=contar)
hilo2 = threading.Thread(target=contarb)
hilo3 = threading.Thread(target=contarb)
hilo4 = threading.Thread(target=saca)
hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()
