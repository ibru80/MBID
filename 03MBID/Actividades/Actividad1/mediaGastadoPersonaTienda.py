#!/usr/bin/python3

import sys 

'''
Mapper de persona - tienda - gasto medio
''' 

#Por cada linea calculamos los pares < persona;tienda;gastomedi > 
for linea in sys.stdin: 
	datos = linea.split(';') 
	print("%s\t%s\t%s" % (datos[0], datos[1], datos[2]) )
