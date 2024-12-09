#!/usr/bin/python3

import sys 

 

''' 

Mapper de Wordcount 

Creado por Jesus Moran 

''' 

#Por cada linea calculamos los pares <palabra, 1> 

for linea in sys.stdin: 
	palabras = linea.split() 
	for palabra in palabras: 
		print("%s\t%s" % (palabra, 1)) 
