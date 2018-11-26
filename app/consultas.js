/*
DanielFranciscoBastarricaLacalle y GabrielSellésSalvà declaramos que esta solución es
fruto exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna
otra persona ni hemos obtenido la solución de fuentes externas, y tampoco hemos compartido
nuestra solución con nadie. Declaramos además que no hemos realizado de manera
deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
resultados de los demás.
*/




//Parámetros de configuración.
var bbdd= "sgdi_pr3";
var filmsCollectionName="peliculas"
var usersCollectionName="usuarios"

var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";


/* AGGREGATION PIPELINE */

/*
Listado de país-número de películas rodadas en él, ordenado por número de películas
descendente y en caso de empate por nombre país ascendente.
*/
function agg1(){
	/* 
		1-Realizamos el unwind para obtener un documento por país del atributo "pais".  
		2-Agrupamos documentos por país y calculamos cuantos documentos hay por cada país. Se almacena el resultado en el atributo "count".
		3-Ordenamos el resultado por número de apariciones de país (de forma descendiente) y, en caso de empate, por nombre del país (de forma ascendente).
	*/
	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;

	  db.db(bbdd).collection(filmsCollectionName).aggregate( 
	  		[
	  			{
	  				$unwind: {
	  						path: "$pais",
	  						preserveNullAndEmptyArrays: true
	  				}
	  			},

	  			{
	  				$group: {
	  						_id: {pais:"$pais"},
	  						count: {$sum: 1}
	  				}
	  			},

	  			{
	  				$sort: {
	  					count: -1,
	  					"_id.pais":1
	  				}

	  			}

	  		]
	  	).toArray(function(err, result) {
	    if (err) throw err;
	    console.log(result);
	    db.close();
	  });
	});
}

/*
Listado de los 3 tipos de película máss populares entre los usuarios de los ’Emiratos
Árabes Unidos’, ordenado de mayor a menor número de usuarios que les gusta. En caso de
empate a número de usuarios, se usa el tipo de película de manera ascendente.
*/
function agg2(){
	/*
		1- Obtenemos los documentos cuyo país (en dirección) es "Emiratos Árabes Unidos".
		2- Obtenemos un documento por gusto que aparezca en el atributo "gustos" de cada instancia.
		3- Agrupamos los documentos por gustos y obtenemos el número de documentos con el mismo valor en dicho atributo.
		4- Ordenamos los resultados tal y como se describe en el enunciado.
		5- Cogemos los 3 resultados con mayor valor en "count". Desechamos el resto.
	 */
	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;

	  db.db(bbdd).collection(usersCollectionName).aggregate( 
	  		[
	  			{ $match:{
	  					"direccion.pais":'Emiratos Árabes Unidos'
	  				}
	  			},
				{
		  			$unwind: {
		  						path: "$gustos",
		  						preserveNullAndEmptyArrays: true
		  			}
		  		},
		  		{
		  			$group: {
		  						_id: {gustos:"$gustos"},
		  						count: {$sum: 1}
		  			}
		  		},

	  			{
	  				$sort: {
	  					count: -1,
	  					"_id.gustos": 1
	  				}

	  			},
	  			
	  			{ 
	  				$limit : 3 

	  			}


	  		]
	  	).toArray(function(err, result) {
	    if (err) throw err;
	    console.log(result);
	    db.close();
	  });
	});
}
  

/*
Listado de país-(edad mínima, edad-máxima, edad media) teniendo en cuenta únicamente
los usuarios mayores de edad, es decir, con más de 17 años. Los países con menos de 3
usuarios mayores de edad no deben aparecer en el resultado.
*/
function agg3(){
  /*
		1- Obtenemos los documentos cuyo valor en "edad" sea mayor que 17.
		2- Agrupamos los documentos por país del atributo dirección. Calculamos el número de instancias de cada país, 
		la media de las edades, el valor mínimo y el valor máximo.
		3- Obtenemos los documentos resultantes que tengan mínimo 3 apariciones.
   */
  MongoClient.connect(url, function(err, db) {
	  if (err) throw err;


	  db.db(bbdd).collection(usersCollectionName).aggregate( 
	  		[
	  			{ $match:{
	  					"edad": { $gt: 17}
	  				}
	  			},

	  			{
		  			$group: 
		  			{
	  						_id: {"pais":"$direccion.pais"},
	  						minAge: { $min: "$edad"},
	  						maxAge:  { $max: "$edad"},
	  						avgAge: { $avg: "$edad"},
	  						count: {$sum: 1}
		  			}
		  		},

	  			{ $match:{
	  					"count": { $gt: 2}
	  				}
	  			}

	  		]
	  	).toArray(function(err, result) {
	    if (err) throw err;
	    console.log(result);
	    db.close();
	  });

   });

}  
  


/*
Listado de título película-número de visualizaciones de las 10 películas más vistas,
ordenado por número descencente de visualizaciones. En caso de empate, romper por título de
película ascendente.
*/

function agg4(){
  /* 
	1- Obtenemos un documento por visualización que aparezca en la lista visualizaciones.
	2- Agrupamos los documentos por el título que aparece en el campo visualizaciones. Calculamos el número de veces que aparace cada título.
	3- Ordenamos los resultados tal y como se describe en el enunciado.
	4- Obtenemos los 10 mejores resultados. Desechamos el resto.
  */

	MongoClient.connect(url, function(err, db) {
		  if (err) throw err;


		  db.db(bbdd).collection(usersCollectionName).aggregate( 
		  		[
			  		{
			  			
			  			$unwind: {
			  						path: "$visualizaciones",
			  						preserveNullAndEmptyArrays: true
			  			}
		  			},
		  			{
			  			
			  			$group: 
			  			{
		  						_id: {"titulo":"$visualizaciones.titulo"},
		  						visualizaciones: {$sum: 1}
			  			}
			  		
			  		},
			  		{
		  				$sort: {
		  					visualizaciones:-1,
		  					"_id.titulo":1		
		  				}

		  			},
		  			
		  			{ 
		  				$limit : 10 

		  			}
		  		
		  		]
		  	).toArray(function(err, result) {
		    if (err) throw err;
		    console.log(result);
		    db.close();
		  });

	   });



}


//Ejecuciones
agg1();
agg2();
agg3();
agg4();

  
/* MAPREDUCE */  
  
// 1.- 
function mr1(){
	/* */
}

// 2.- 
function mr2(){
	/* */
}

// 3.- 
function mr3(){
	/* */
}

// 4.- 
function mr4(){
	/* */
}

