#!/usr/bin/python

import sys 

'''
Reducer de persona - tienda - gasto medio
''' 

subproblema = None
contador = 0

#Por cada linea calculamos los pares < persona;tienda;gastomedi > 
for linea in sys.stdin: 
    clave, gasto, count = linea.split("\t", 1)

    