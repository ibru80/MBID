#!/usr/bin/python

import sys

'''
Reducer de persona - tienda - gasto medio
Vamos a utilizar el combiner como un agrupador
no como un reductor, ya que el resultado de la media se puede ver
alterado o distorsionado en el reducer.
''' 

acumulador_datos = set() # creamos un set para evitar que dentro de la lista contengan datos duplicados
patente_actual = None

#Por cada linea calculamos los pares cita patentes
for linea in sys.stdin: 
    patente, citada = linea.replace('\n', '').split("\t") # clave array

    if patente_actual != patente and patente_actual != None:
        patente_actual = patente
        # ordenamos el array
        print("%s\t%s" % (patente_actual, ",".join(acumulador_datos)) )
        acumulador_datos = set()
    else:
        if patente_actual == None:
            patente_actual = patente # momento inicial
    
    # anyadimos en el array la concatenacion de los datos por clave
    acumulador_datos.add(citada)

# imprimimos los datos de la ultima iteracion
print("%s\t%s" % (patente_actual, ",".join(acumulador_datos)) )
