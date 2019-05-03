// interoperating with RDDs
// convert RDDs to Datasets

// -------------------------------------------------
// 1. reflection to infer schema

import spark.implicits._

// define path
val path_txt = "C:\\Users\\Xiaoxiao\\Documents\\people.txt"

// define class
case class Person(name: String, age: Long)

// create an RDD of Person object(a case class which define the schema of the table) from a text file, 
// and then convert it to a DataFrame
val peopleDF = spark.sparkContext.textFile(path_txt).map(_.split(",")).map(attributes => Person(attributes(0),attributes(1).trim.toInt)).toDF()

//Register DF as a temporary view
peopleDF.createOrReplaceTempView("peopleDF")

// use sql function
val seniorDF = spark.sql("select name,age from peopleDF where age >= 50")

// the columns of a ROW in the result can be accessed by Field Index
seniorDF.map(senior => "Name: " + senior(0)).show()

// or by Field Name
seniorDF.map(senior => "Name: " + senior.getAs[String]("name")).show()



// ----------------------------------------------------
// 2. programmatically infer a schema: 
//    1) Create an RDD of Rows from the original RDD;
// 	  2) Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
// 	  3) Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.


import org.apache.spark.sql.types._

//create an RDD
val peopleRDD = sc.textFile(path_txt)

//schema encoded in a string
val schemaString = "name age"

//generated schema based on schemaString
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

//convert records of the RDD into ROWs
import org.apache.spark.sql.Row
val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0),attributes(1).trim))

//apply schema to RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)

//create temporary view
peopleDF.createOrReplaceTempView("people3")























