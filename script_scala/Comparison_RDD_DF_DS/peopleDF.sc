// create DataFrame

val path = "C:\\Users\\Xiaoxiao\\Documents\\people.json"
val peopleDF = spark.read.json(path)


// ----------------------------------------------
// Untyped Operations
// This import is needed to use the "$-notation"
import spark.implicits._

// print schema
peopleDF.printSchema()

// select column
peopleDF.select("name").show()
peopleDF.select("name","age").show()
peopleDF.select($"name",$"age"+1).show()

// filter
peopleDF.filter($"age">20).show()
peopleDF.filter(peopleDF("age")>20).show()
peopleDF.filter($"name" === "Andy").show()
peopleDF.filter(peopleDF("name") === "Andy").show()

// group 
peopleDF.groupBy("age").count().show()


// ----------------------------------------------
// running SQL query programmatically
// register the DataFrame as a SQL temporary view
// sql function enables applications to run SQL queries programmatically
// and return a DataFrame

peopleDF.createOrReplaceTempView("people")

val sqlDF = spark.sql("select * from people")