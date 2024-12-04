#!/usr/bin/python3

import sys 


''' 
Reducer de MaxTemp 
Creado por Jesus Moran 
''' 


subproblema = None 

tempMaxima = None 


for claveValor in sys.stdin: 
	anyo, temp = claveValor.split("\t", 1) 
	
	#convertimos la temp a float 
	temp = float(temp) 
	
	#El primer subproblema es el primer anyo de reducer (y la temp maxima de momento tambien)  
	if subproblema == None: 
		subproblema = anyo 
		tempMaxima = temp 
	
	#si el anyo es del subrpoblema actual, comprobamos si es la temperatura maxima 
	if subproblema == anyo: 
		if temp > tempMaxima: 
			tempMaxima = temp 
	else: #si ya acabamos con el subproblema, emitimos            
		print("%s\t%s" % (subproblema, tempMaxima)) 

		#Pasamos al siguiente subproblema (de momento la temp es la maxima) 
		subproblema = anyo 
		tempMaxima = temp 


#el anterior bucle no emite el ultimo subproblema
print("%s\t%s" % (subproblema, tempMaxima)) 
