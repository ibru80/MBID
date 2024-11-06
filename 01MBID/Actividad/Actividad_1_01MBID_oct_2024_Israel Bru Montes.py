#pip install pymongo twython pandas

# %%
"""
Ejemplo de importación de Social Media en Mongo DB

Importación de twits en una base de datos Mongo.


ANTES DE EJECUTAR ESTE SCRIPT, ES NECESARIO:
    1. Tener instancia en la nube de MongoDB Atlas, MongoDB instalado en local o en un servidor propio.

EL CÓDIGO SE DIVIDO EN 3 PARTES
    1. Importación de los paquetes de python necesarios
    2. Configurar la base de datos Mongo y las colecciones
    3. Ejemplos de manipulación
"""


###### PARTE 1: Importación de los paquetes de python necesarios  ######
import pandas as pd
import pymongo #Necesario instalarlo la primera vez de forma aislada: pip install Twython
from twython import Twython #Necesario instalarlo la primera vez de forma aislada: pip install Twython
from datetime import datetime as dt

###### PARTE 2: Configurar la base de datos Mongo y las colecciones ######
#Establecimiento Conexión a MongoDB Atlas

# Datos de ejemplo, necesario modificar por vuestra instancia en MongoDB Atlas o local
dbStringConnection = "mongodb+srv://ibru:ibm200380@cluster0.zrguc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

dbName = 'Twitter_Actividad_1'
dbCollectionA = 'tweetsAccount'
dbCollectionT = 'tweets'

client = pymongo.MongoClient(dbStringConnection)

# Definición de la base de datos MongoDB
db = client[dbName]

# Colección accounts en la base de datos para las cuentas de twitter
accounts = db[dbCollectionA]

# Colección donde tenemos los tweets
tweets = db[dbCollectionT]




# %%
''' 
    4.1.- En la colección de cuentas de twitter, tener los campos amigos y tweets enviados,
        cargar los datos correspondientes mediante consulta mongodb + código python.
'''

pipeline_calcula_tweets_amigos = [
    {
        '$group': {
            '_id': '$user.screen_name',
            'c_total_tweets': {'$sum': '$user.statuses_count'},
            'c_friends': {'$sum': '$user.friends_count'}
        }
    },
    {
        '$addFields': {
            'new_total_tweets': '$c_total_tweets',
            'new_friends': '$c_friends'
        }
    }
]

for org in tweets.aggregate(pipeline_calcula_tweets_amigos):
    print(org)
    accounts.update_one(
        {
            'Twitter_handle': org['_id']
        },
        {
            '$set': { "num_tweets_enviados": org['new_total_tweets']},
            '$set': { "num_amigos": {'$push': {'$each': org['new_friends']}}}
        }
    )

# %%
'''
4.2.- En la colección de tweets, calcular la antigüedad para cada tweet en función de la fecha actual considerando antigüedad 0 el día de hoy 
y sumando +1 por cada día transcurrido. Incluir en el mismo documento del tweet un nuevo campo que se llamará antiguedad_dias con esa antigüedad calculada.
'''

# obtenemos la fecha de hoy para calcular la antiguedad
hoy = dt.now()

# recorremos la tabla completa
for tweet in tweets.find():

    # Recuperamos la fecha de hoy y la del tweet para calcular cuantos dias han pasado
    fecha_pub = dt.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
    antiguedad = (hoy - fecha_pub).days

    #actualizamos el tweet con la nueva columna
    result = tweets.update_many(
        {'id': tweet['id']},
        {
            '$set': { "antiguedad": antiguedad},
            '$set': { "date_pub": fecha_pub},
            '$set': { "day_of_week_pub": fecha_pub.strftime('%A')} # Para el ejercicio 5.7 añadimos la columna del día de la semana de publicacion
        }
    )

    print(result)


# %%
'''
4.3.- En la colección de tweets, calcular la antigüedad de cada tweet relativa 
con la fecha de creación de la cuenta más antigua
 de la colección actual de datos. Considerando antigüedad 0 si fue 
 enviado el mismo día de creación y sumando +1 por cada día transcurrido 
 desde entonces en función de la fecha del tweet. 
Incluir en el mismo documento del tweet un nuevo campo se llamará 
frescura_relativa_dias.
'''
min_created_at_user = ""
for t in tweets.find():
    fecha = dt.strptime(t['user']['created_at'], "%a %b %d %H:%M:%S +0000 %Y") 
    if (min_created_at_user == '' or fecha < min_created_at_user):
        min_created_at_user = fecha
        print(min_created_at_user)

for tweet in tweets.find():    
    # Calculamos la frescura_relativa_dias respecto de la fecha de la cuenta más antigua
    fecha_pub = dt.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
    frescura_relativa_dias = (fecha_pub - min_created_at_user).days

    result = tweets.update_one(
        {'id': tweet['id']},
        {'$set': { "frescura_relativa_dias": frescura_relativa_dias}}
    )

    print(result)

# %%
###### PARTE 3: Exemplos de manipulación. ######

pipeline_promedio_hashtags = [
    {
        '$group': {
        '_id': '$user.screen_name',
        'total_hashtags': {"$sum": {"$size": "$entities.hashtags"}},
        'total_tweets': {'$sum': 1},
        }
    },
    {
        '$addFields': {
            'promedio_hashtags': {
                '$divide': ['$total_hashtags', '$total_tweets']
            }
        }
    }
]

for org in tweets.aggregate(pipeline_promedio_hashtags):
    print(org)
    accounts.update_one(
        {
            'Twitter_handle': org['_id']
        },
        {
            "$set":{"promedio_hashtags": org['promedio_hashtags']}
        }
    )

# %%
# Obtener la hora actual en formato UTC
current_time = datetime.datetime.now()

# Obtener documentos de la colección de Tweets
documents = tweets.find()

# Iterar a través de los documentos
for document in documents:
	# Extraer la fecha de creación del tweet y convertirla a un objeto datetime
	created_at_str = document['created_at']
	created_at = datetime.datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)

  # Calcular diferencia fechas del tweet en minutos
	diferencia = (current_time - created_at).total_seconds()/60

  # Actualizar el documento con el campo de diferencia
	document['DiferenciaMinutos'] = int(diferencia)
	tweets.update_one({'_id': document['_id']}, {'$set': {'DiferenciaMinutos': document['DiferenciaMinutos']}})

	# Imprimir ID del tweet y su diferencia
	print(f"ID del tweet: {document['_id']}, DiferenciaMinutos: {document['DiferenciaMinutos']} minutos")


