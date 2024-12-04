#!/usr/bin/python3

import sys 
 
''' 
Reducer de Wordcount 
Creado por Jesus Moran 
''' 
 
subproblema = None 
suma_conteos = 0 

for claveValor in sys.stdin: 
	palabra, conteo_palabra = claveValor.split("\t", 1) 
 
	#convertimos conteo a entero 
	conteo_palabra = int(conteo_palabra) 

	#El primer subproblema es la primer palabra  
	if subproblema == None: 
		subproblema = palabra 
	
	#si la palabra es del subproblema actual, sumamos 
	if subproblema == palabra: 
		suma_conteos = suma_conteos + conteo_palabra 
	else: #si ya acabamos con el subproblema, emitimos           
		print("%s\t%s" % (subproblema, suma_conteos)) 
		
		#Pasamos al siguiente subproblema 
		subproblema = palabra 
		suma_conteo = conteo_palabra 

#el anterior bucle no emite el ultimo subproblema    
print("%s\t%s" % (subproblema, suma_conteos)) 
