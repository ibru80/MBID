#!/usr/bin/python

import sys

'''
Reducer de persona - tienda - gasto medio
Vamos a reducir los datos que recibimos agrupados por el combiner
nos viene la clave por un lado (persona-tienda) 
''' 

acumulador_datos = []
clave_actual = None

#Por cada linea calculamos los pares < persona-tienda gastomedi > 
for linea in sys.stdin: 
    clave, gasto = linea.split("\t") # clave array

    # recomponemos el array que nos envia el combiner
    datos = gasto.replace('\n', '').split(',')

    if clave_actual != clave and clave_actual != None:
       clave_actual = clave
       # Calculamos la media
       media_gasto = 0
       if len(acumulador_datos) > 0:
        media_gasto = sum( int(x) for x in acumulador_datos) / len(acumulador_datos)
       print("%s\t%s" % (clave_actual, round(media_gasto, 2 ) ))
       acumulador_datos = []
    else:
        if clave_actual == None:
            clave_actual = clave # momento inicial
    
    # anyadimos en el array la concatenacion de los datos por clave
    acumulador_datos = acumulador_datos + datos

# imprimimos los datos de la ultima iteracion
# Calculamos la media
media_gasto = 0
if len(acumulador_datos) > 0:
    media_gasto = sum( int(x) for x in acumulador_datos) / len(acumulador_datos)
print("%s\t%s" % (clave_actual, round(media_gasto, 2 ) ))