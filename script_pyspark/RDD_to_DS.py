

#---------------------------------------------------
# 2. programmaticallly inger a schema
#---------------------------------------------------

# import data types
from pyspark.sql.types import *

# load a text file and convert each line into a Row
# test file converted to a list, and each line in the text converted to an element in the list
lines = sc.textFile("C:\\Users\\Xiaoxiao\\Documents\\people.txt")
# each line converted to a list
parts = lines.map(lambda l:l.split(","))
# each line converted to a tuple
people = parts.map(lambda p:(p[0], p[1].strip()))

# schema
schemaString = "name age"

fields = [StructField(field_name, StringType(),True) for field_name in schemaString.split()]
schema = StructType(fields) 

# apply schema to RDD
schemaPeople = spark.createDataFrame(people, schema)

# create temperary view
schemaPeople.createOrReplaceTempView("people")

#
results = spark.sql("select * from people")
results.show()