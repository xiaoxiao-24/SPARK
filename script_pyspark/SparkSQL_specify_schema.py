from pyspark.sql import Row
# exemple: http://spark.apache.org/docs/latest/sql-getting-started.html#interoperating-with-rdds

# Load a text file and convert each line to a Row.
# 1.read file as a collection of lines
lines = sc.textFile("C:\\Users\\Xiaoxiao\\Documents\\people.txt")
# 2.convert each line to a row
parts = lines.map(lambda l: l.split(","))
# 3.
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people) # runtime error on windows
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)
# Name: Justin


# ------------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
	
df = spark.read.json("examples/src/main/resources/people.json") # runtime error on windows