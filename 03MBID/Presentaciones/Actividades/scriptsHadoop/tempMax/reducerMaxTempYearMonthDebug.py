#!/usr/bin/python3

import sys  

''' 
Reducer de MaxTemp 
Creado por Jesus Moran 
''' 

subproblema = None 
tempMaxima = None 

numReducer = 1
print("-----------------------") 
print("Reducer " + str(numReducer)) 
numReducer = numReducer + 1 

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
		print("    Input: " + claveValor) 

		if temp > tempMaxima: 
			tempMaxima = temp 

	else: #si ya acabamos con el subproblema, emitimos            
		print("        Output: %s\t%s" % (subproblema, tempMaxima)) 

		#Pasamos al siguiente subproblema (de momento la temp es la maxima) 
		print("-----------------------")         
		print("Reducer " + str(numReducer)) 
		numReducer = numReducer + 1     

		print("    Input: " + claveValor)     
		subproblema = [anyo, month]
		tempMaxima = temp 

#el anterior bucle no emite el ultimo subproblema   
print("        Output: %s\t%s" % (subproblema, tempMaxima)) 
