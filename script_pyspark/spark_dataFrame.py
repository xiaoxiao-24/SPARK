from pyspark.sql import SQLContext
from pyspark.sql.types import *

# Spark SQL StructType
# The data type representing rows. A StructType object comprises a list of StructFields.

# Spark SQL StructField:
# Represents a field in a StructType. A StructField object comprises three fields, name (a string), dataType (a DataType) and nullable (a bool). 


#initialize our SQLContext
sqlContext = SQLContext(sc)

taxiFile = sc.textFile("file:///Users/xiaoxiao/Downloads/nyctaxisub.csv")
taxiFile.take(5)
taxiFile.top(5) # both functions get the first 5 lines

# steps to build a data frame
# get header
header = taxiFile.first()
# clean data, remove the double quotes
schemaString = header.replace('"','')
# get fields to build schema (by using the head line)
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]
# check how many fields there is 
len(fields)
# set the right data type for each column
fields[2].dataType = TimestampType()
fields[3].dataType = FloatType()
fields[4].dataType = FloatType()
fields[7].dataType = IntegerType()
fields[8].dataType = TimestampType()
fields[9].dataType = FloatType()
fields[10].dataType = FloatType()
fields[11].dataType = IntegerType()
fields[13].dataType = FloatType()
fields[14].dataType = IntegerType()
# get rid of the underscores in the fields who have it
fields[0].name = 'id'
fields[1].name = 'rev'

# construct schema which will be used to build data frame
schema = StructType(fields)

# seprate header line
taxiHeader = taxiFile.filter(lambda l: '_id' in l)
# remove header line with function subtract()
# *attention: here we can't use the variable header above, because it's a local variable, it can't be subtracted from an RDD
taxiNoHeader = taxiFile.subtract(taxiHeader)
taxiNoHeader.count() # get number of lines

# methode 1: create dataframe through a temp RDD
# import new modules for correctly dealing with datetimes
from datetime import *
from dateutil.parser import parse
# split the row content with the appropriate seperator and chain the right data type
taxi_temp = taxiNoHeader.map(lambda k: k.split(",")).map(lambda p: (p[0],p[1], parse(p[2].strip('"')), float(p[3]),float(p[4]), p[5], p[6], int(p[7]), parse(p[8].strip('"')), float(p[9]), float(p[10]), int(p[11]), p[12], float(p[13]), int(p[14]), p[15]))
# build the data frame
taxi_df = sqlContext.createDataFrame(taxi_temp, schema)

# methode 2: create dataframe using the toDF() function
taxi_df = taxiNoHeader.map(lambda k: k.split(",")).map(lambda p: (p[0].strip('"'),p[1].strip('"'), parse(p[2].strip('"')), float(p[3]),float(p[4]), p[5].strip('"'), p[6].strip('"'), int(p[7]), parse(p[8].strip('"')), float(p[9]), float(p[10]), int(p[11]), p[12].strip('"'), float(p[13]), int(p[14]), p[15].strip('"'))).toDF(schema)


# check and use some pandas function
taxi_df.groupBy("rate_code").count().show()
taxi_df.groupBy("vendor_id").count().show()

# get info from schema
taxi_df.dtypes
taxi_df.printSchema()

# use sql instead of data frame
# first register the dataframe as a named temporary table
taxi_df.registerTempTable("taxi")
sqlContext.sql("SELECT vendor_id,COUNT(*) FROM taxi GROUP BY vendor_id").show()
