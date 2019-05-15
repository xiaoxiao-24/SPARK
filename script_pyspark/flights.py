
# ------------------------------------------------------------------
# define file path
airlinesPath="/user/amy_ds/sample_dataset/airlines.csv"
airportsPath="/user/amy_ds/sample_dataset/airports.csv"
flightsPath="/user/amy_ds/sample_dataset/flights.csv"

# 1. import file to RDD
flights=sc.textFile(flightsPath)
flights.count()


# 2. create schema
from pyspark.sql.types import *
schema = StructType([
	StructField("date",DateType(),True),
	StructField("airline",IntegerType(),True),
	StructField("flightnum",IntegerType(),True),
	StructField("origin",StringType(),True),
	StructField("dest",StringType(),True),
	StructField("dep",TimestampType(),True),
	StructField("dep_delay",DoubleType(),True),
	StructField("arv",TimestampType(),True),
	StructField("arv_delay",DoubleType(),True),
	StructField("airtime",DoubleType(),True),
	StructField("distance",DoubleType(),True)
])


# 3. prepare RDD 
from datetime import datetime

DATE_FMT = "%Y-%m-%d"
TIMESTAMP_FMT = "%Y-%m-%d %H%M"

def concat(a,b,c):                                                                            
	return a+b+c 

flights_split = flights.map(lambda x:x.split(","))
flights_tuple = flights_split.map(lambda x:(datetime.strptime(x[0],DATE_FMT).date(),
											int(x[1]),
											int(x[2]),
											x[3],
											x[4],
											datetime.strptime(concat(x[0],' ',x[5]),TIMESTAMP_FMT),
											float(x[6]),
											datetime.strptime(concat(x[0],' ',x[7]),TIMESTAMP_FMT),
											float(x[8]),
											float(x[9]),
											float(x[10])))


# 4. transform RDD to DataFrame
flignts_df = spark.createDataFrame(flights_tuple, schema) 
flignts_df.first()  
flights_df.show(5)