#!/usr/bin/python

import sys 

'''
Mapper de persona - tienda - gasto medio
''' 

#Por cada linea calculamos los pares < persona;tienda;gastomedi > 
for linea in sys.stdin: 
	datos = linea.split(',') 
	# Con las 2 primeras columnas creamos una clave con cada valor, con guion 
	print("%s\t%s" % (datos[0]+"-"+ datos[1], datos[2].replace("\n","") )) # Concatenamos el split separado por tabulaciones