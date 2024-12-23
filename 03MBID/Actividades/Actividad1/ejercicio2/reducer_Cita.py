#!/usr/bin/python

import sys

'''
Reducer de patentes citadas 
Recogemos los datos de combiner y agrupamos/ordenamos por la lista de patentes citadas
''' 

acumulador_datos = set()
patente_actual = None

#Por cada linea calculamos los pares cita patentes
for linea in sys.stdin: 
    patente, citada = linea.replace('\n', '').split("\t") # clave array

    # Anyadimos la lista de patentes que citan, la patente principal para evitar que existan duplicados
    acumulador_datos.update(citada.split(','))    

    if patente_actual != patente and patente_actual != None:
        patente_actual = patente
        # ordenamos el array
        citados = ",".join(str(n) for n in sorted(acumulador_datos))
        print("%s\t%s" % (patente_actual, citados ))
        acumulador_datos = set()
    else:
        if patente_actual == None:
            patente_actual = patente # momento inicial
    
    # anyadimos en el array la concatenacion de los datos por clave
    acumulador_datos.add(citada)

# imprimimos los datos de la ultima iteracion
citados = ",".join(str(n) for n in sorted(acumulador_datos))
print("%s\t%s" % (patente_actual, citados ))