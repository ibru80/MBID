#!/usr/bin/python3

import sys 

from pyspark.sql import SparkSession 

''' 
Programa creado por Jesus Moran 
Este programa cuenta el numero de apariciones de cada palabra 

''' 
 
#inicializacion 
spark = SparkSession.builder.appName('miWordCount').getOrCreate()  

entrada = sys.argv[1] 
salida = sys.argv[2] 

#cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

#hacemos el conteo de cada palabra 
conteo = datosEntrada.flatMap(lambda linea: linea.split(" ")).map(lambda palabra: (palabra, 1)).reduceByKey(lambda x, y: x + y) 

#guardamos la salida 
conteo.saveAsTextFile(salida) 
