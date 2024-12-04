#!/usr/bin/python3

import sys  

''' 
Reducer de MaxTemp 
Creado por Jesus Moran 
''' 

subproblema = None 
tempMaxima = None 


for claveValor in sys.stdin: 
	anyo, month, temp = claveValor.split("\t", 2) 

	#convertimos la temp a float 
	temp = float(temp) 

	#El primer subproblema es el primer anyo-mes de reducer (y la temp maxima de momento tambien)  
	if subproblema == None: 
		subproblema = [anyo, month]
		tempMaxima = temp 

	#si el anyo-mes es del subrpoblema actual, comprobamos si es la temperatura maxima 
	if subproblema == [anyo, month]: 
		if temp > tempMaxima: 
			tempMaxima = temp 

	else: #si ya acabamos con el subproblema, emitimos            
		print("%s\t%s\t%s" % (subproblema[0], subproblema[1], tempMaxima)) 

		#Pasamos al siguiente subproblema (de momento la temp es la maxima) 
		subproblema = [anyo, month]
		tempMaxima = temp 

#el anterior bucle no emite el ultimo subproblema   
print("%s\t%s\t%s" % (subproblema[0], subproblema[1], tempMaxima)) 
