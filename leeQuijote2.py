from pyspark import SparkContext
import sys
import string
from random import random

#Eliminamos los signos de puntuación y contamos la cantidad de palabras de una linea
def wordsInLine(line):
    for c in string.punctuation+"¿!«»":
        line = line.replace(c,'')
    return len(line.split())

#Tomamos aleatoriamente un quinto de las lineas del fichero y las escribe en un nuevo fichero
def copiaLineasAleatorias(fichero, salidaLineasAleatorias):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(fichero)
        rdd  = data.map(lambda x: [x, random()]).filter(lambda x : x[1] < 0.2).map(lambda x: x[0])
        with open(salidaLineasAleatorias, 'w') as f:
            for r in rdd.collect():
                f.write(r + '\n') # Hay que añadir salto de linea porque al parecer no quedan incluidas en el rdd
            
def cuentaPalabras(fichero, salidaCuentaPalabras):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(fichero)
        rdd = data.map(lambda x: wordsInLine(x))
        mensaje = f'Numero de palabras en {fichero} es :{rdd.sum()}'
        with open(salidaCuentaPalabras, 'w') as f:
            f.write(mensaje)

def main(fichero, resume = False):   
    salidaLineasAleatorias = fichero[0:-4]+'_s05.txt'
    salidaCuentaPalabras = 'out_' + fichero   
    if resume:
        copiaLineasAleatorias(fichero, salidaLineasAleatorias) 
        fichero = salidaLineasAleatorias
    cuentaPalabras(fichero, salidaCuentaPalabras)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2]=='resume')
    else:
        main()
