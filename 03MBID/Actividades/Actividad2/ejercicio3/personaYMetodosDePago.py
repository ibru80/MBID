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
salida1 = os.path.dirname(__file__) + "/comprasSinTDCMayorDe1500.txt"#sys.argv[2]
salida2 = os.path.dirname(__file__) + "/comprasSinTDCMenoroIgualDe1500.txt"#sys.argv[2] 

# cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

# procesamos los datos de entrada e invocamos la funcion que filtra las lineas que no cumplen que tienen al menos 2 elementos despues de hacer split separados por ;
# después creamos una tupla con la clave persona. Filtramos el RDD por aquellos gasto de personas que NO se han pagado con tarjeta de credito
RDD_Filtrado = datosEntrada.map(lambda linea: linea.split(";")).filter(lambda x: len(x) > 2 and x[1] != "Tarjeta de Crédito" ).map(lambda x: ( x[0] +"-"+ x[1], x[2] )) # Maria-Bizum 142

# Hacemos la suma de gasto por metodo de pago por persona y redondeamos
RDD_reduce = RDD_Filtrado.reduceByKey(lambda x, y: round(float(x) + float(y), 2) )
#RDD_reduce = RDD_Filtrado.groupBy(0, 1).sum()

# Vamos a extraer aquellas personas que al menos tienen un gasto de más de 1500
RDD_mas_1500 = RDD_reduce.filter(lambda x: x[2] >= 1500)

#RDD_2_metodos = RDD_mas_1500.

# Vamos a extraer aquellas personas que al menos tienen un gasto de más de 1500
RDD_menos_1500 = RDD_reduce.filter(lambda x: x[2] < 1500)

# Reducimos las tuplas a persona y gasto, reducimos por clave y sumamos el gasto
#RDD_reduce = RDD_Filtrado.map(lambda x: (x[0], x[2]) ).reduceByKey(lambda x, y: round(float(x) + float(y), 2) )

#guardamos la salida 
RDD_reduce.saveAsTextFile(salida1) 
