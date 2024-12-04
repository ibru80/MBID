#!/usr/bin/python3

import sys 

''' 
Mapper de maxTemp 
Creado por Jesus Moran 
''' 

#Por cada medida de temp emitimos los pares <{anyo, mes}, temp> (emitirmos los tres datos) 
for linea in sys.stdin: 
	linea = linea.strip() 
	anyo , mes, temp = linea.split("\t", 2) 
	print("%s\t%s\t%s" % (anyo, mes, temp)) 


