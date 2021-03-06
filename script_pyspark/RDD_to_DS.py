#---------------------------------------------------
# 1. a Row schema(si on sait déjà le schema)
#---------------------------------------------------
from pyspark.sql import Row


Person = Row('name','age') # specify the schema

schemaPerson = StructType([
	StructField("name",StringType(),True),
	StructField("age",IntegerType(),True)
])

people_lines_RDD = sc.textFile("/user/amy_ds/sample_dataset/people.txt") # read text as an RDD
people_RDD = people_lines_RDD.map(lambda a:a.split(","))  # split champ, each line is a list
people_tuple = people_RDD.map(lambda a:(a[0],a[1])) # make each line a tuple
person = people_tuple.map(lambda r:Person(*r)) # apply schema "Row" to each line
person_df = spark.createDataFrame(person)
person_df = spark.createDataFrame(people_RDD, Person)
person_df = spark.createDataFrame(people_tuple, Person)
person_df = person.toDF()   # 4 ways to transform an RDD to a DataFrame

person_df = spark.createDataFrame(people_RDD, schemaPerson)

# order by
person_df.orderBy(["age","name"],ascending=[0,1]).show()   
person_df.orderBy(person_df.age.desc()).show()
from pyspark.sql.functions import *                                                            
person_df.orderBy(desc("age"),"name").collect() 

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