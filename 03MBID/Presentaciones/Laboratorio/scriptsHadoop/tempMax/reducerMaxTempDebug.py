#!/usr/bin/python3

import sys  
import os 

''' 
Reducer de MaxTemp 
Creado por Jesus Moran 
''' 

subproblema = None 
tempMaxima = None 

tarea = os.environ.get('mapreduce_task_partition')
print("-----------------------")
print("-----------------------")
print("Tarea " + str(tarea))

numReducer = 1
print("\t-----------------------")
print("\tReducer " + str(numReducer)) 
numReducer = numReducer + 1 

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
		print("\t\tEntrada: " + claveValor) 

		if temp > tempMaxima: 
			tempMaxima = temp 

	else: #si ya acabamos con el subproblema, emitimos            
		print("\t\t\tSalida: %s\t%s" % (subproblema, tempMaxima)) 

		#Pasamos al siguiente subproblema (de momento la temp es la maxima) 
		print("\t-----------------------")         
		print("\tReducer " + str(numReducer)) 
		numReducer = numReducer + 1     

		print("\t\tEntrada: " + claveValor)     
		subproblema = anyo
		tempMaxima = temp 

 

#el anterior bucle no emite el ultimo subproblema   
print("\t\t\tSalida: %s\t%s" % (subproblema, tempMaxima))

print("-----------------------")
print("-----------------------") 
