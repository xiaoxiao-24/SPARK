from pyspark import SparkContext #, SparkConf

#conf = SparkConf()
#conf = SparkConf().setMaster("local").setAppName("My App")
#conf = (SparkConf().setMaster("local").setAppName("My app").set("spark.executor.memory", "1g"))

sc = SparkContext()
#sc = SparkContext(conf = conf)

flightsPath="file:///Users/xiaoxiao/Documents/BigData/udemy_sample/cours/Spark/FlightsData/flights.csv"

# 1. create a RDD from a file
flights=sc.textFile(flightsPath)

ct = flights.count()
print(ct)