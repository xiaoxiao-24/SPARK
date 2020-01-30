// ----------------------
// JSON to DataFrame
// ----------------------

// import Spark Session into Apache Spark
import org.apache.spark.sql.SparkSession

// create a new Spark Session 'spark' using 'build()' function
val spark = SparkSession.builder().appName("Spark SQL Basic").config("spark.some.config.optin","somevalue").getOrCreate()

// create a DataFrame 'df' by import data from json file from HDFS
val df = spark.read.json("/user/xiaoxiao/employee.json")

// show result
df.show
df.printSchema

// basics SQL operations on DataFrame 'df'
df.select("name").show
df.select($"age"+3).show
df.filter($"age" > 30).show
df.filter(line => line(1) == "John").show
df.groupBy("age").count().show

// Create a temporary view 'employee' of 'df' DataFrame so we can use sql directly
df.createOrReplaceTempView("employee")
// display the table 'employee' into 'sqlDF'
val sqlDF = spark.sql("select * from employee")
sqlDF.show



// ---------------------------
// CSV to DataFrame
// ---------------------------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

// 1. create schema
val customSchema = StructType(Seq(
StructField("userId",StringType,true),
StructField("color",StringType,true),
StructField("count",IntegerType,true)))

// 2. import CSV
val resultAsACsvFormat = spark.read.schema(customSchema).option("delimiter",",").csv("C:\\Users\\Xiaoxiao\\Documents\\Git\\SPARK\\script_scala\\Comparison_RDD_DF_DS\\testevent.csv")

// 3. group
val finalResult = resultAsACsvFormat.groupBy("color").sum("count")

// 4. output
val outputDF = finalResult.collect()



// ---------------------------
// Text file to DataFrame
// ---------------------------

import spark.implicits._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder // for RDD, who use encoders for serialization

// 1. Creating a class ‘Employee’ to store name and age of an employee.
case class Employee(name:String, age:Long)

// 2. create DF by reading from text file
val path_text = "/user/xiaoxiao/employee.txt"
val employeeDF = spark.sparkContext.textFile(path_text).map(_.split(",")).map(lines => Employee(lines(0),lines(1).trim.toInt)).toDF()

// 3. create tmp view
employeeDF.createOrReplaceTempView("employeeDFview")

// 4. define a DF with SparkSQL
val youngstersDF = spark.sql("select * from employeeDFview where age between 18 and 30")

youngstersDF.map(youngster => "name:" + youngster(0)).show
youngstersDF.map(youngster => "age:" + youngster(1)).show
youngstersDF.map(youngster => "name:" + youngster(0) + ", age:" + youngster(1)).show