/*
DanielFranciscoBastarricaLacalle y GabrielSellésSalvà declaramos que esta solución es
fruto exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna
otra persona ni hemos obtenido la solución de fuentes externas, y tampoco hemos compartido
nuestra solución con nadie. Declaramos además que no hemos realizado de manera
deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
resultados de los demás.
*/



/*
	PARAMETROS DE EJECUCIÓN

	node consultas.js <tipo> <problema>

	donde:

	1-tipo:
		agg: problemas de Aggregation Pipelines descritos en el enunciado.
		mr: problemas de Map Reduce descritos en el enunciado.
	
	2-problema:
		1: problema 1
		2: problema 2
		...

	Ejemplos de ejecución:

	node.js consultas.js mr 3 => ejecución del problema 3 de Map Reduce.
	node.js consultas.js agg 1 => ejecución del problema 1 de Aggregation Pipelines.

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
  
/* MAPREDUCE */  
  
/*
	Listado de países-número de películas rodadas en él.
*/
function mr1(){
	/* 
	-Map: emitimos cada país de cada película.
	-Reduce: Sumamos todos los valores, dando lugar al número de películas rodadas en cada país.
	*/

		MongoClient.connect(url, function(err, db) {
		  if (err) throw err;

		  db.db(bbdd).collection(filmsCollectionName).mapReduce( 
		  		
		  		//Map
		  		function(){ 
		  			var i=0;
		  			for ( i=0;i < this.pais.length; i++){
		  				emit(this.pais[i],1);	
		  			}	
		  		},
		  		
		  		//Reduce
		  		function(key,values){
		  			return (key,Array.sum(values));
		  		}, 
		  		
		  		{
		 			out: { inline: 1 }
		  		},
		  		function (err, result) {
			       if (err) throw err;
			       console.log(result);
			       db.close();
			    }		  	
		  	);
	   });
}

/*
Listado de rango de edad-número de usuarios. Los rangos de edad son periodos de 10 añoos:
[0; 10), [10; 20), [20; 30), etc. Si no hay ningun usuario con edad en un rango concreto dicho rango
no deberá aparecer en la salida.
*/
function mr2(){
	/*
	-Map: Obtenemos el intervalo de edad de cada usuario. 
	-Reduce: Sumamos todos los valores, dando lugar al número de usuarios de cada rango de edad.
	 */

	MongoClient.connect(url, function(err, db) {
		  if (err) throw err;

		  db.db(bbdd).collection(usersCollectionName).mapReduce( 
		  		
		  		//Map
		  		function(){ 
		  			
		  			var i= this.edad - this.edad%10;
		  			var j= i+10;
		  			var interval= "["+i.toString()+","+j.toString()+")";

		  			emit(interval,1);	
		  		},
		  		
		  		//Reduce
		  		function(key,values){
		  			return (key,Array.sum(values));
		  		}, 
		  		
		  		{
		 			out: { inline: 1 }
		  		},
		  		function (err, result) {
			       if (err) throw err;
			       console.log(result);
			       db.close();
			    }		  	
		  	);
	   });
}

/*
Listado de países-(edad mínima, edad-máxima, edad media) teniendo en cuenta únicamente
los usuarios con más de 17 años.
*/ 
function mr3(){
	/*
	-Map: Devolvemos la media, edad mínima y edad máxima de cada usuario. Como solo hay una edad, 
	las tres tienen el mismo valor. Esto se realiza ya que, en el caso de que la clave solo aparezca una
	vez, no se aplicará la función reduce.
	-Reduce: Calculamos la media, la edad mínima y la edad máxima para cada país.
	 */

	MongoClient.connect(url, function(err, db) {
		  if (err) throw err;

		  db.db(bbdd).collection(usersCollectionName).mapReduce( 
		  		
		  		//Map
		  		function(){ 
		  			if(this.edad>17){
		  				//Si solo aparece una vez en la clave, no va al reduce.
		  				var dict={ avg: this.edad, min: this.edad, max:this.edad};
		  				emit(this.direccion.pais, dict);
		  			}
		  		},
		  		
		  		//Reduce
		  		function(key,values){
		  			//Calculamos el valor mínimo, el máximo y la media.
					var v,i, avg=0, min=10000, max=-10000;

		  			for (i=0; i<values.length; i++){
		  				v=values[i]["avg"];
		  				avg += v;
		  				if(min>v) min=v;
		  				if(max<v) max=v;
		  			}

		  			return (key,{avg:avg/values.length, min:min, max:max});
		  		}, 
		  		{ 
		  			out: { inline: 1 }
		  		},
		  		
		  		function (err, result) {
			       if (err) throw err;
			       console.log(result);
			       db.close();
			    }
		  	);	  	
	   });
}

/*
Listado de año-número de visualizaciones veraniegas, donde una ((visualización veraniega))
es aquella que se ha producido en los meses de junio, julio o agosto.
*/
function mr4(){
	/* 
	-Map: Obtenemos el mes y el año de cada visualización. Si la visualización se produjo en 
	junio, julio o agosto, la emitimos.
	-Reduce: Sumamos todos los valores, dando lugar al número visualizaciones veraniegas 
	asoaciadas a cada año.
	*/
	MongoClient.connect(url, function(err, db) {
		  if (err) throw err;

		  db.db(bbdd).collection(usersCollectionName).mapReduce( 
		  		
		  		//Map
		  		function(){ 
		  			var year;
		  			var month;
		  			var i
		  			for ( i=0; i<this.visualizaciones.length; i++){//x in this.visualizaciones){
		  				x=this.visualizaciones[i]["fecha"];
		  				month=x.substring(5,7);
		  				if(parseInt(month)>5 && parseInt(month)<9 ){
			  				year=x.substring(0,4);
		  					emit(year,1);
		  				}
		  			}
		  		},
		  		
		  		//Reduce
		  		function(key,values){
		  			return (key,Array.sum(values));
		  		}, 
		  		{ 
		  			out: { inline: 1 }
		  		},
		  		
		  		function (err, result) {
			       if (err) throw err;
			       console.log(result);
			       db.close();
			    }
		  	);	  	
	   });
}


//Ejecución
function mrProblem(num){
	if(num==1) mr1();
	else if(num==2) mr2();
	else if(num==3) mr3();
	else if(num==4) mr4();
	else console.log("Error: en Map Reduce solo hay implementados 4 problemas. Introduzca mr {1..4} para ver los resultados");
}


function aggProblem(num){
	if(num==1) agg1();
	else if(num==2)	agg2();
	else if(num==3)	agg3();
	else if(num==4)	agg4();
	else console.log("Error: en Aggregation Pipeline solo hay implementados 4 problemas. Introduzca agg {1..4} para ver los resultados");
}


//Se va a ejecutar como main.
if(process.argv[2]=="mr"){
	mrProblem(process.argv[3]);
}else if(process.argv[2]=="agg"){
	aggProblem(process.argv[3]);
}else{
	console.log("Opción introducida no válida.");
}

