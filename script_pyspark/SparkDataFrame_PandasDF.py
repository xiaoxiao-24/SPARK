# import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create a new SparkSession called my_spark
my_spark = SparkSession.builder.getOrCreate()

# Verifier the new SparkSession
print(my_spark)

# check the data inside the cluster
# SparkSession has an attribute called catalog which lists all the data inside the cluster
# "catalog" has one most useful methode called .listTables(), which return the names of all the tables in the cluster as a list
print(spark.catalog.listTables())


# query the table in the cluster
query = "SELECT * FROM flights LIMIT 10"
flights10 = spark.sql(query) # get the 10 first rows
flights10.show()

# take the table from the cluster and work it locally using pandas
# method: toPandas() 
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"
flight_counts = spark.sql(query)
pd_counts = flight_counts.toPandas()
print(pd_counts.head())

# put a pandas DataFrame into a Spark cluster (to a Spark DataFrame), SparkSession method: .createDataFrame(). The output of this method is stored locally, not in SparkSession catalog. This means you can use all the Spark DataFrame methods on it, but you can't access the data in other contexts.
# register the DataFrame as a temporary table in the catalog, Spark DataFrame method: .createTempView() or .createOrReplaceTempView()
import pandas as pd
import numpy as np
pd_temp = pd.DataFrame(np.random.random(10))
spark_temp = spark.createDataFrame(pd_temp)
print(spark.catalog.listTables()) # Examine, this DF is not in the tables in the catalog yet
spark_temp.createOrReplaceTempView("spark_temp")
print(spark.catalog.listTables()) # Examine again, this DF is now in the tables in the catalog

# read directly from a CSV file
file_path = "/usr/local/share/datasets/airports.csv"
airports = spark.read.csv(file_path,header=True)
airports.show()





