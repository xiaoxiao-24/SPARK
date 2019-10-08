# verify SparkContext
print(sc)

# print Spark version
print(sc.version)

# to use Spark DataFrame, we need create a SparkSession object from SparkContext
# SparkContext: like a connection to the cluster
# SparkSession: like an interface of that connection,it's often called spark
# import SparkSession from pyspark.sql

#----------------------
# for spark 2.0
#----------------------
from pyspark.sql import SparkSession
# create a SparkSession
my_spark = SparkSession.builder.getOrCreate()
print(my_spark)
# to liste all the data(tables) inside the cluster
# use an attribute of SparkSession: catalog
# and a method: .listTables()
print(spark.catalog.listTables())
# query
query = "FROM flights SELECT * LIMIT 10"
# Get the first 10 rows of flights
flights10 = spark.sql(query)
# Show the results
flights10.show()
# Convert spark DataFrame to a pandas DataFrame
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"
flight_counts = spark.sql(query)
pd_counts = flight_counts.toPandas()
# to read csv
file_path = "/usr/local/share/datasets/airports.csv"
airports = spark.read.csv(file_path, header=True)

#----------------------
# for spark 1.6
#----------------------
from pyspark.sql import SQLContext
# initialize our SQLContext
sqlContext = SQLContext(sc)
print(sqlContext)
# query
query = "FROM flights SELECT * LIMIT 10"
# Get the first 10 rows of flights
flights10 = sqlContext.sql(query)
# Show the results
flights10.show()
# Convert the results to a pandas DataFrame
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"
flight_counts = sqlContext.sql(query)
pd_counts = flight_counts.toPandas()
# to read csv
taxiFile = sc.textFile("file:///Users/xiaoxiao/Downloads/nyctaxisub.csv")

#-----------------------
# for spark 1.6 for hive
#-----------------------
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
print(hive_context)
hive_context.sql("show databases").show()
# Convert the results to a pandas DataFrame: toPandas()
query = "select * from cis_compo_bdpm limit 10"
cis_df = hive_context.sql(query)
cis_df.show()
cis_pd = cis_df.toPandas()