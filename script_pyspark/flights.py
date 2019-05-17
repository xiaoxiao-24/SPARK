# define file path
airlinesPath="/user/amy_ds/sample_dataset/airlines.csv"
airportsPath="/user/amy_ds/sample_dataset/airports.csv"
flightsPath="/user/amy_ds/sample_dataset/flights.csv"


# ------------------------------------------------------------------
# partie 1: flights
# ------------------------------------------------------------------

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
											str(x[3]),
											str(x[4]),
											datetime.strptime(concat(x[0],' ',x[5]),TIMESTAMP_FMT),
											float(x[6]),
											datetime.strptime(concat(x[0],' ',x[7]),TIMESTAMP_FMT),
											float(x[8]),
											float(x[9]),
											float(x[10])))


# 4. transform RDD to DataFrame
flights_df = spark.createDataFrame(flights_tuple, schema) 
flights_df.first()  
flights_df.show(5)


# ------------------------------------------------------------------~
# partie 2: airlines
# ------------------------------------------------------------------
airlines = sc.textFile(airlinesPath)
airlines_wo_header = airlines.filter(lambda x:"Description" not in x)
airlines_split = airlines_wo_header.map(lambda x:x.split(","))
airlines_tuple = airlines_split.map(lambda x:(int(x[0].replace('"','')), str(x[1].replace('"','')))) 
	# enlever les '"' dans les colonnes, and transforme les colonnes à sa vrai datatype

airlines_header = airlines.first().split(",")
schema_string = [str(x) for x in airlines.first().split(",")] # [str(x) for x in airlines_header]
schema_airlines = StructType([
	StructField(schema_string[0], IntegerType(), True),
	StructField(schema_string[1], StringType(), True)
])

airlines_df = spark.createDataFrame(airlines_tuple,schema_airlines)

# ------------------------------------------------------------------~
# partie 3: airports
# ------------------------------------------------------------------
airports = sc.textFile(airportsPath)  
airports_wo_header = airports.filter(lambda x:"Description" not in x) 
airports_split = airports_wo_header.map(lambda x:x.split('","')) 
airports_tuple = airports_split.map(lambda x:(str(x[0].replace('"','')),str(x[1].replace('"',''))))

airports_header = airports.first().split(",")
schema_string_airport = [str(x) for x in airports_header]
schema_string_airport = [str(x) for x in airports.first().split(",")]

schema_airports = StructType([
	StructField(schema_string_airport[0], StringType(), True),
	StructField(schema_string_airport[1], StringType(), True)
])

schema_airports = StructType([StructField(x, StringType(), True) for x in schema_string_airport])

airports_df = spark.createDataFrame(airports_tuple, schema_airports)


# ------------------------------------------------------------------~
# partie 4: dataFrame operations
# ------------------------------------------------------------------

# *Filter*: filtre les depart delay au dessus de 1000
flights_df.filter(flights_df['dep_delay'] >= 1000).count() 

# *Join*: join "flights_df" with "airlines_df" by 'airline code'
flights_df.filter(flights_df['dep_delay'] >= 1000).join(airlines_df, flights_df.airline==airlines_df.Code, 'inner').show()
flights_PPG_df = flights_df.filter(flights_df['origin']=="PPG")
airports_PPG_df = airports_df.filter(airports_df['Code']=="PPG")
flights_PPG_df.join(airports_df, flights_PPG_df.origin==airports_df.Code, 'inner').show()

# *Sort*: sort a column (3 ways)
flights_df.orderBy(flights_df.dep_delay.desc()).show(10)
flights_df.sort(flights_df.dep_delay.desc()).show(10)
flights_df.sort("dep_delay", ascending = False).show(10)
from pyspark.sql.functions import *
flights_df.sort(desc("dep_delay")).show(5)
flights_df.orderBy(desc("dep_delay"),"arv_delay").show(5)
flights_df.orderBy(desc("dep_delay"),asc("arv_delay")).show(5)
flights_df.orderBy(["dep_delay","arv_delay"], ascending=[0,1]).show(5)

# *Select* & *groupBy* & *sort*
#  select la colonne pour groupBy, ensuite sort le resultat de groupBy en descendant 
flights_origin = flights_df.select("origin").groupBy("origin").count()
flights_origin.sort(desc("count")).show(5)


# ------------------------------------------------------------------~
# partie 5: RDD operations : ByKey
# ------------------------------------------------------------------

# reduceByKey
flights_origin_pair = flights_split.map(lambda x:(x[3],1)) # create (k,v) pair, take the origin column
flights_origin_pair_count = flights_origin_pair.reduceByKey(lambda a,b:a+b) 
flights_origin_pair_count.sortByKey().take(10)  # sort the key alphabetically






