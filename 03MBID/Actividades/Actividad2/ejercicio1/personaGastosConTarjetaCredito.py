#!/usr/bin/python

import sys

from pyspark.sql import SparkSession 

''' 
Este programa cada persona indique la suma del dinero gastado con tarjeta de crédito, con el formato
persona;gastoconTDC
''' 

#inicializacion 
spark = SparkSession.builder.appName('personaGastosConTarjetaCredito').getOrCreate()  

entrada = sys.argv[1] 
salida = sys.argv[2] 

# cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

# procesamos los datos de entrada e invocamos la funcion que filtra las lineas que no cumplen que tienen al menos 3 valores en split separados por ;
# y también filtramos por los que en la 2a columna tiene el valor de metodo de pago "Tarjeta de Credito" 
RDD_Filtrado = datosEntrada.map(lambda linea: linea.split(";")).filter(lambda x: len(x) > 2 and x[1] == "Tarjeta de Crédito")

# Reducimos las tuplas a persona y gasto, reducimos por clave y sumamos el gasto
RDD_reduce = RDD_Filtrado.map(lambda x: (x[0], x[2]) ).reduceByKey(lambda x, y: round(float(x) + float(y), 2) )

#guardamos la salida de las particiones del RDD
RDD_reduce.saveAsTextFile(salida) 
