#!/usr/bin/python

import sys

'''
Combiner de persona - tienda - gasto medio
Vamos a utilizar el combiner como un agrupador
no como un reductor, ya que el resultado de la media se puede ver
alterado o distorsionado en el reducer.
''' 

clave_actual = None
acumulador_datos = []

#Por cada linea calculamos los pares < persona-tienda   gasto > 
for linea in sys.stdin: 
    clave, gasto = linea.split("\t")

    #quitamos el retorno de carro que viene en el ultimo dato
    gasto = gasto.replace("\n","")

    # Cuando clave actual es distinto de la clave, cambiamos la clave e imprimimos el acumulado
    if clave != clave_actual and clave_actual != None:
        clave_actual = clave
        print("%s\t%s" % (clave_actual, ",".join(acumulador_datos) ) )
        acumulador_datos = [] # borramos los datos anteriores
    else:
        if clave_actual == None:
            clave_actual = clave # momento inicial
    
    # anyadimos en el array la concatenacion de los datos por clave
    acumulador_datos.append(gasto)

# imprimimos los datos de la ultima iteracion
print("%s\t%s" % (clave_actual, ",".join(acumulador_datos) ) )
