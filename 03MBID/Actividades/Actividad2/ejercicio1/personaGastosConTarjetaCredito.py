#!/usr/bin/python

import sys 
import os

from pyspark.sql import SparkSession 

''' 
Este programa cada persona indique la suma del dinero gastado con tarjeta de crédito, con el formato
persona;gastoconTDC
''' 

def filtroTarjetaDeCredito(record):
    '''
    Función que filtra los valores 
    '''
    if record[1] == "Tarjeta de Crédito": #Solo devolvemos la linea que contiene como medio de pago tarjeta de credito
        return [ record[0], record[2] ]


 
#inicializacion 
spark = SparkSession.builder.appName('personaGastosConTarjetaCredito').getOrCreate()  

entrada = os.path.dirname(__file__) + "/persona_medio_pago_gasto.csv" #sys.argv[1] 
salida = os.path.dirname(__file__) + "/salida3.txt"#sys.argv[2] 

#cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

# procesamos los datos de entrada e invocamos la funcion que filtra los datos 
RDD_Filtrado = datosEntrada.map(lambda linea: linea.split(";")).filter(lambda x: x[1] == "Tarjeta de Crédito").map(lambda x: (x[0], x[2]) ).reduceByKey(lambda x, y: float(x) + float(y))

#guardamos la salida 
RDD_Filtrado.saveAsTextFile(salida) 
