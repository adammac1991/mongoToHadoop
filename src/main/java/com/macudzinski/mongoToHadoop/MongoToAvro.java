package com.macudzinski.mongoToHadoop;
import static org.apache.spark.sql.functions.max;

import java.util.Date;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;

import org.apache.hadoop.conf.Configuration;

import com.mongodb.hadoop.MongoInputFormat;
import org.bson.BSONObject;



/**
 * A spark application that queries a given Mongodb database collection, and transforms the resulting
 * data into avro which are then written to a given filesystem path.
 * 
 * @param args[0] Query start date in the following date format YYYY-MM-DDTHH:mm:ss.mmm<+/-Offset>.
 * @param args[1] Query end date in the following date format YYYY-MM-DDTHH:mm:ss.mmm<+/-Offset>.
 * @param args[2] Mongo URI in form "mongodb://<username>:<password>@<host>:<Port>+.<dbName>.<collectionName>);
 * @oaram args[3] Output file path
 */
public class MongoToAvro {
	public static void main(String[] args ){
		String startDate = args[0];
		String endDate = args[1];
		//Of form "mongodb://<username>:<password>@<host>:<Port>+.<dbName>.<collectionName>);"
		String mongoURI = args[2];
		String outputFilePath = args[3];
		
		//Set Spark configuration and context
		SparkConf conf = new SparkConf().setAppName("MongoToSpark-" + startDate);
		JavaSparkContext sc = new JavaSparkContext(conf);
		String queryString = "{updateDate : {$gt : ISODate(" + startDate + ")}, {$lte : ISODate(" 
							+ endDate + ")}}";	

		Configuration mongodbConfig = new Configuration();
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		//Set Mongo URI
		mongodbConfig.set("mongo.input.uri", mongoURI);
		mongodbConfig.set("mongo.input.query", queryString);
		
		//Create Input RDD from Mongo
		JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(mongodbConfig,    
		        														MongoInputFormat.class,  
		        														Object.class,             
		        														BSONObject.class);
		SQLContext sqlContext = new SQLContext(sc);
		
		//Here we transform the BSONRDD into a RDD<String>, a required input to
		//the .json() method which converts it into a dataframe based on the inferred schema.
		//Ideally we should create a mapper function that generates row objects according
		//to a predefined schema. This way we can directly call the BSONObject get()
		//method, which is much more efficient than parsing the json for each attribute.
		JavaRDD<BSONObject> bsonRDD = documents.values();
		JavaRDD<String> jsonRDD = bsonRDD.map(new Function<BSONObject, String>() {
		    					public String call(BSONObject bson) {
		    							return bson.toString(); 
		    					}});
		DataFrame ordersDF = sqlContext.read().json(jsonRDD);
		
		ordersDF.write().format("com.databricks.spark.avro").save(outputFilePath);
		}
}
	

