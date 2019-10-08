// start a Spark Session and create a DataFrame

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