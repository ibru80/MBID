#!/usr/bin/python3 

import sys 
import os 

''' 
Mapper de maxTemp 
Creado por Jesus Moran 
''' 

tarea = os.environ.get('mapreduce_task_partition')

#Por cada medida de temp emitimos los pares <anyo, temp> 
numLinea = 0
entrada = ""
salida = ""
for linea in sys.stdin:
	numLinea = numLinea + 1
	entrada = linea
	linea = linea.strip() 
	anyo , mes, temp = linea.split("\t", 2) 
	salida = str(anyo) + "\t" + str(temp)
	
	print("Tarea " + tarea + " Linea " + str(numLinea) + " Entrada: " + entrada)
	print("Tarea " + tarea + " Linea " + str(numLinea) + " Salida: " + salida)
