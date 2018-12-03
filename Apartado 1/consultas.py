# -*- coding: utf-8 -*-

'''
Daniel Bastarrica Lacalle y Gabriel Sellés Salvà declaramos que esta solución es
fruto exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna
otra persona ni hemos obtenido la solución de fuentes externas, y tampoco hemos com-
partido nuestra solución con nadie. Declaramos además que no hemos realizado de manera
deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
resultados de los demás.
'''

from pymongo import MongoClient

from bson.objectid import ObjectId

# 1. Fecha y título de las primeras 'n' peliculas vistas por el usuario 'user_id'
# usuario_peliculas( 'fernandonoguera', 3 )
def usuario_peliculas(user_id, n):
	return db.usuarios.find( { "_id": user_id }, { "_id":0, "visualizaciones":{"$slice" : n} , "visualizaciones.fecha":1, "visualizaciones.titulo":1 })


# 2. _id, nombre y apellidos de los primeros 'n' usuarios a los que les gusten 
# varios tipos de película 'gustos' a la vez
# usuarios_gustos(  ['terror', 'comedia'], 5  )
def usuarios_gustos(gustos, n):
	return db.usuarios.find( {"gustos":{"$all": gustos}} , {"_id":1, "nombre":1, "apellido1":1, "apellido2":1} ).limit(n)


# 3. _id de usuario de sexo 'sexo' y con una edad entre 'edad_min' e 'edad_max' incluidas
# usuario_sexo_edad('M', 50, 80)
def usuario_sexo_edad( sexo, edad_min, edad_max ):
	return db.usuarios.find( {"sexo":sexo, "edad":{"$gte":edad_min, "$lte":edad_max} }, {"_id":1} )


# 4. Nombre, apellido1 y apellido2 de los usuarios cuyos apellidos coinciden,
#    ordenado por edad ascendente
# usuarios_apellidos()
def usuarios_apellidos():
	return db.usuarios.find( {"$expr": {"$eq":["$apellido1","$apellido2"]} }, {"_id":0, "nombre":1, "apellido1":1, "apellido2":1} ).sort( "edad", pymongo.ASCENDING )

# 5.- Titulo de peliculas cuyo director empiece con un 'prefijo' dado
# pelicula_prefijo( 'Yol' )
def pelicula_prefijo( prefijo ):
	return db.peliculas.find( {"director": {"$regex" : '^'+prefijo} } , {"_id":0, "titulo":1})


# 6.- _id de usuarios con exactamente 'n' gustos, ordenados por edad descendente
# usuarios_gustos_numero( 6 )
def usuarios_gustos_numero(n):
	#buscamos usuarios con 'n' documentos dentro del array 'visualizaciones' 
	return db.usuarios.find( {"gustos" : {"$size":n } }, {"_id":1} ).sort("edad", pymongo.DESCENDING)


# 7.- usuarios que vieron la pelicula 'id_pelicula' en un periodo 
#     concreto 'inicio' - 'fin'
# usuarios_vieron_pelicula( '583ef650323e9572e2812680', '2015-01-01', '2016-12-31' )
def usuarios_vieron_pelicula(id_pelicula, inicio, fin):
	#Buscamos usuarios que tengan al menos un documento dentro de 'visualizaciones' que encaje con todos los criterios
	return db.usuarios.find( {"visualizaciones" : { "$elemMatch": { "_id" : ObjectId(id_pelicula), "fecha":{"$gte":inicio ,"$lte":fin} } } } , {"_id":1} )

# Según el valor de la variable 'printExtendido' del main imprimirá cada documento de la consulta en una linea o 
# 	'desgranará' los resultados para intentar mostrarlos de una forma más legible
def imprimeCursor( cursor ) : 
	for doc in  cursor :
		if not printExtendido : 
			print(doc)
		else :
			for k, v in doc.items():
				if type(v) == type(list()) and len(v) > 1:
					print("{", k, ":")
					for pos in v:
						print("\t",pos)
					print("\t}")
				else:
					print("{",k,":",v,"}")
		print()

if __name__ == "__main__" :
	client = MongoClient()
	db = client.sgdi_pr3
	
	printExtendido = False
	
	#Apartado 1
	'''
	print("Busqueda usuario_peliculas")
	imprimeCursor(  usuario_peliculas( 'fernandonoguera', 3 )  ) 
	'''
	
	#Apartado 2
	'''
	print("Busqueda usuarios_gustos")
	imprimeCursor( usuarios_gustos(  ['terror', 'comedia'], 5  ) ) 
	'''
	
	#Apartado 3
	'''
	print("Busqueda usuario_sexo_edad")
	imprimeCursor(  usuario_sexo_edad('M', 50, 80) ) 
	'''
	
	#Apartado 4
	'''
	print("Busqueda usuarios_apellidos")
	imprimeCursor(  usuarios_apellidos() ) 
	'''
	
	#Apartado 5
	'''
	print("Busqueda pelicula_prefijo")
	imprimeCursor( pelicula_prefijo( 'Yol' )  ) 
	'''
	
	#Apartado 6
	'''
	print("Busqueda usuarios_gustos_numero")
	imprimeCursor( usuarios_gustos_numero( 6 )   ) 
	'''
	
	#Apartado 7
	'''
	print("Busqueda usuarios_vieron_pelicula")
	imprimeCursor( suarios_vieron_pelicula( '583ef650323e9572e2812680', '2015-01-01', '2016-12-31' )  ) 
	'''

	
