#!/usr/bin/python3 

import sys 
from pyspark.sql import SparkSession


''' 
Programa creado por Jesus Moran 
Este programa cuenta el numero de apariciones de cada palabra 
''' 

#funcion que utilizo en el map: por cada palabra emite <palabra, 1> 
def obtenerPalabrasUno(linea): 
	paresClaveValor = [] 	 

	#genero los pares <clave, valor> 
	palabras = linea.split(" ") 
	for palabra in palabras: 
		claveValor = (palabra, 1) 
		paresClaveValor.append(claveValor) 

	#emito los pares <clave, valor> 
	return paresClaveValor 

 

#funcion que utilizo en reduce: sumo cada valor de la clave 
def obtenerSuma(valor1, valor2): 
	return valor1 + valor2 

#inicializacion 
spark = SparkSession.builder.appName('miWordCount').getOrCreate()

entrada = sys.argv[1] 
salida = sys.argv[2] 

#cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

#hacemos el conteo de cada palabra 
conteo = datosEntrada.flatMap(obtenerPalabrasUno).reduceByKey(obtenerSuma) 

#guardamos la salida 
conteo.saveAsTextFile(salida) 
