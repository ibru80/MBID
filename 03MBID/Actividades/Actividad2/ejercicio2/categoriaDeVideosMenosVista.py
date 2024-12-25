#!/usr/bin/python

import sys 
import os

from pyspark.sql import SparkSession 

''' 
 crear un programa llamado CategoriaDeVideosMenosVista que
obtenga cuál es la categoría de videos menos vista de la plataforma Youtube y el número total de
visualizaciones que hay en esa categoría. 
Los datos de entrada están en los archivos 0.txt, 1.txt, etc y cada fila contiene la información de un
video tabulada con el siguiente formato: id del video de youtube, usuario que subió el video, número de
días desde que se subió el video y la fecha en la que obtuvieron los datos, categoría del video, longitud
del video, número de visitas del video, puntuaci ón del video, número de puntuaciones del video,
núm ero de comentarios del video, y una lista de ids de videos relacionados
''' 
 
#inicializacion 
spark = SparkSession.builder.appName('categoriaDeVideosMenosVista').getOrCreate()  

entrada = os.path.dirname(__file__) + "/persona_medio_pago_gasto.csv" #sys.argv[1] 
salida = os.path.dirname(__file__) + "/salida3.txt"#sys.argv[2] 

#cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada) 

# procesamos los datos de entrada e invocamos la funcion que filtra los datos 
RDD_Filtrado = datosEntrada.map(lambda linea: linea.split(";")).filter(lambda x: x[1] == "Tarjeta de Crédito").map(lambda x: (x[0], x[2]) ).reduceByKey(lambda x, y: float(x) + float(y))

#guardamos la salida 
RDD_Filtrado.saveAsTextFile(salida) 
