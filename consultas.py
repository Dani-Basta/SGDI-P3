# -*- coding: utf-8 -*-

'''
Daniel Bastarrica Lacalle y Gabriel Sellés Salvà declaramos que esta solución es
fruto exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna
otra persona ni hemos obtenido la solución de fuentes externas, y tampoco hemos com-
partido nuestra solución con nadie. Declaramos además que no hemos realizado de manera
deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
resultados de los demás.
'''

import pymongo

from pymongo import MongoClient

# 1. Fecha y título de las primeras 'n' peliculas vistas por el usuario 'user_id'
# usuario_peliculas( 'fernandonoguera', 3 )
def usuario_peliculas(user_id, n):
	return db.users.find( { "_id": user_id }, { "_id":0, "visualizaciones.fecha":1 , "visualizaciones.titulo":1 }).limit(n)


# 2. _id, nombre y apellidos de los primeros 'n' usuarios a los que les gusten 
# varios tipos de película 'gustos' a la vez
# usuarios_gustos(  ['terror', 'comedia'], 5  )
def usuarios_gustos(gustos, n):
	return db.users.find( {"gustos":{"$all": gustos}} , {"_id":1, "nombre":1, "apellido1":1, "apellido2":1} ).limit(n)


# 3. _id de usuario de sexo 'sexo' y con una edad entre 'edad_min' e 'edad_max' incluidas
# usuario_sexo_edad('M', 50, 80)
def usuario_sexo_edad( sexo, edad_min, edad_max ):
	return db.users.find( {"sexo":sexo, "edad":{"$gte":edad_min, "$lte":edad_max} }, {"_id":1} )


# 4. Nombre, apellido1 y apellido2 de los usuarios cuyos apellidos coinciden,
#    ordenado por edad ascendente
# usuarios_apellidos()
def usuarios_apellidos():
	#return db.users.find( {"$where": "function(){ return this['apellido1'] == this['apellido2'];}"}, {"_id":0, "nombre":1, "apellido1":1, "apellido2":1} ).sort({"edad":1})
	return db.users.find( {"$expr": {"$eq":["$apellido1","apellido2"]} }, {"_id":0, "nombre":1, "apellido1":1, "apellido2":1} ).sort({"edad":1})

# 5.- Titulo de peliculas cuyo director empiece con un 'prefijo' dado
# pelicula_prefijo( 'Yol' )
def pelicula_prefijo( prefijo ):
	return db.films.find( {"titulo":"/^"+prefijo+"/"}, {"_id":0, "titulo":1})


# 6.- _id de usuarios con exactamente 'n' gustos, ordenados por edad descendente
# usuarios_gustos_numero( 6 )
def usuarios_gustos_numero(n):
	#return db.users.find( {"$where": "function(){ return this['gustos'].length == "},{"_id":1}).sort({"edad":-1})
	return db.users.find( {"gustos" : {"$size":n } }, {"_id":1} ).sort({"edad":-1})


# 7.- usuarios que vieron pelicula la pelicula 'id_pelicula' en un periodo 
#     concreto 'inicio' - 'fin'
# usuarios_vieron_pelicula( '583ef650323e9572e2812680', '2015-01-01', '2016-12-31' )
def usuarios_vieron_pelicula(id_pelicula, inicio, fin):
	return db.users.find( {"visualizaciones._id":id_pelicula, "visualizaciones.fecha":{"$gte":inicio ,"$lte":fin}} , {"_id":1} )
