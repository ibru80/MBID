#!/usr/bin/python

import sys 
import os

from pyspark.sql import SparkSession 

''' 
Dado un dataset que contenga entradas con la forma
“persona;método_pago;dinero_gastado”, crea un programa llamado personaYMetodosDePago
''' 

#inicializacion 
spark = SparkSession.builder.appName('personaGastosConTarjetaCredito').getOrCreate()  

entrada = os.path.dirname(__file__) + "/persona_medio_pago_gasto.csv" #sys.argv[1] 
salida1 = os.path.dirname(__file__) + "/comprasSinTDCMayorDe1500.txt"
salida2 = os.path.dirname(__file__) + "/comprasSinTDCMenoroIgualDe1500.txt"

# cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

# procesamos los datos de entrada e invocamos la funcion que filtra las lineas que no cumplen que tienen al menos 2 elementos despues de hacer split separados por ;
# después creamos una tupla con la clave persona. Filtramos el RDD por aquellos gasto de personas que NO se han pagado con tarjeta de credito
RDD_Filtrado = datosEntrada.map(lambda linea: linea.split(";")).filter(lambda x: len(x) > 2 and x[1] != "Tarjeta de Crédito" )

# Vamos a extraer aquellas personas que al menos tienen un gasto de más de 1500
# y contamos las veces que han pagado
RDD_mas_1500 = RDD_Filtrado.filter(lambda x: float(x[2]) >= 1500).countByKey()

# imprimimos el primero ya que esta ordenado en orden ascendente
with open(salida1, 'w') as out:
    for elem in RDD_mas_1500:
        out.write( elem +";"+ str(RDD_mas_1500.get(elem)) +"\n" )

# Vamos a extraer aquellas personas que al menos tienen un gasto de menos o igual de 1500 y contamos cuantas compras han hecho
RDD_menos_1500 = RDD_Filtrado.filter(lambda x: float(x[2]) <= 1500).countByKey()

# imprimimos el primero ya que esta ordenado en orden ascendente
with open(salida2, 'w') as out:
    for elem in RDD_menos_1500:
        out.write( elem +";"+ str(RDD_menos_1500.get(elem)) +"\n" )