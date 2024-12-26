#!/usr/bin/python

import sys 
import os

from pyspark.sql import SparkSession 

''' 
 crear un programa llamado CategoriaDeVideosMenosVista que
obtenga cuál es la categoría de videos menos vista de la plataforma Youtube y el número total de
visualizaciones que hay en esa categoría. 
Los datos de entrada están en los archivos 0.txt, 1.txt, etc y cada fila contiene la información de un
video tabulada con el siguiente formato: id del video de youtube (0), usuario que subio el video (1), nuumero de
dias desde que se subio el video (2), categoria del video (3), longitud
del video(4), numero de visitas del video (5), puntuacion del video (6), nuumero de puntuaciones del video (7),
numero de comentarios del video (8), y una lista de ids de videos relacionados (9)
''' 
 
# inicializacion 
spark = SparkSession.builder.appName('categoriaDeVideosMenosVista').getOrCreate()  

entrada = os.path.dirname(__file__) + "/0303" #sys.argv[1] 
salida = os.path.dirname(__file__) + "/salida3.txt"#sys.argv[2] 

# cargamos los datos de entrada 
datosEntrada = spark.sparkContext.textFile(entrada)

# procesamos los datos de entrada y seleccionamos unicamente la columna 3 que indica la categoria y la columna 5 que tiene el numero de visitas
#filtromos aquellas lineas cuyo resultado de split es mayor que 5 para evitar errores al obtener la posicion 3 y 5
RDD_seleccionado = datosEntrada.map(lambda linea: linea.split("\t") ).filter(lambda x: len(x) > 5).map(lambda x: ( x[3], int(x[5]) ) )

# Calculamos la suma de visitas por categoria y ordenamos por número de visitas
RDD_reduce = RDD_seleccionado.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1] )

# calculamos el minimo y convertimos la tupla en un string separado por ";"
element_min = ';'.join(str(val) for val in RDD_reduce.first()).strip()

# imprimimos el primero ya que esta ordenado en orden ascendente
with open(salida, 'w') as out:
    out.write( element_min )
