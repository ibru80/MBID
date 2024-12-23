#!/usr/bin/python

import sys 

'''
Mapper de patentes citadas
''' 


#Por cada linea
#Eliminamos la cabecera de los datos mapeados
for i,linea in enumerate(sys.stdin): 
	if i > 0: #ignoramos la primera linea con la cabecera
		datos = linea.replace("\n","").split(',') #Eliminamos el salto de linea de los datos
		# al mapear invertimos los numeros de patentes citadas por la citante, invertimos los valores de la linea: citada citante
		print("%s\t%s" % (datos[1], datos[0] )) # Concatenamos el split separado por tabulaciones