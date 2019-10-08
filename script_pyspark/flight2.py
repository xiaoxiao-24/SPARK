# define file path
airlinesPath="hdfs:///user/swethakolalapudi/flightDelaysData/airlines.csv"
airportsPath="hdfs:///user/swethakolalapudi/flightDelaysData/airports.csv"
flightsPath="hdfs:///user/swethakolalapudi/flightDelaysData/flights.csv"

# ------------------------------------------------------------------------------
# -  Basic RDD 
# ------------------------------------------------------------------------------
# flight
# 1. create a RDD from a file
flights=sc.textFile(flightsPath)
flights.count()


# 2. write a user define function to parse this row 
#   1). reference these columns by name
#   2). convert these fields to relevant data types from string
#   3). set up a class to represent 1 record
#      then convert each list in flightParsed to this class
from datetime import datetime
from collections import namedtuple

fields   = ('date', 'airline', 'flightnum', 'origin', 'dest', 'dep',
            'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance')
Flight   = namedtuple('Flight', fields, verbose=True)

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"
 
# this function is used to parsed the row list and return a Flight object
def parse(row):
    row[0]  = datetime.strptime(row[0], DATE_FMT).date()
    row[5]  = datetime.strptime(row[5], TIME_FMT).time()
    row[6]  = float(row[6])
    row[7]  = datetime.strptime(row[7], TIME_FMT).time()
    row[8]  = float(row[8])
    row[9]  = float(row[9])
    row[10] = float(row[10])
    return Flight(*row[:11])

flightsParsed=flights.map(lambda x: x.split(',')).map(parse)
# 3. sum the total distance travelled by all flights
#    extract the distance field
totalDistance=flightsParsed.map(lambda x:x.distance).reduce(lambda x,y:x+y)
#    calcule the average distance by a flight
avgDistance=totalDistance/flightsParsed.count()
# 4. % of flights delayed
#    since we use a lot flightsParsed, we force it to be materialized, cache it to memory, make reuse easier
flightsParsed.persist()
flights_dep_delay=flightsParsed.filter(lambda x:x.dep_delay>0).count()/float(flightsParsed.count())
flights_arv_delay=flightsParsed.filter(lambda x:x.arv_delay>0).count()/float(flightsParsed.count())
#    when we are done with this RDD, we discard it from memory
flightsParsed.unpersist()
# 5. compute average delay (in another way of avgDistance)
#    use aggregate function: take an RDD x.delay return a tuple (sum,count)
#    use tuple, with both the sum of total delays and the count
avgDelay=flightsParsed.map(lambda x:x.dep_delay).aggregate((0,0)
                                                          (lambda acc,value:(acc[0]+value,acc[1]+1)),
                                                          (lambda acc1,acc2:(acc1[0]+acc2[0],acc1[1]+acc2[1])))
# 6. compute a frequency distribution of delays
freqDelay=flightsParsed.map(lambda x:int(x.dep_delay/60)).countByValue()


# ------------------------------------------------------------------------------
# -  Pair RDD 
# ------------------------------------------------------------------------------
# 7. comput average delay per airport
#    create a Pair RDD with origin airport and delay for each flight
airportDelays = flightsParsed.map(lambda x:(x.origin,x.dep_delay))
# to access the keys and values
airportDelays.keys().take(5)
airportDelays.values().take(5)

#    option1: reduceByKey
#    the sum delay - the 1st RDD
airportTotalDelay = airportDelays.reduceByKey(lambda x,y:x+y)
#    the count - the 2nd RDD
airportCount = airportDelays.mapValues(lambda x:1).reduceByKey(lambda x,y:x+y)
#    merge sum and count: matching values by same key - join the 2 RDDs
airportSumCount = airportTotalDelay.join(airportCount)
#    divide the sum/count
airportAvgDelay = airportSumCount.mapValues(lambda x:x[0]/float(x[1]))

#    option2: combineByKey
#    create a new RDD
airportSumCount2 = airportDelays.combineByKey((lambda value:(value,1)),
                                              (lambda acc,value:(acc[0]+value, acc[1]+1)),
                                              (lambda acc1,acc2:(acc1[0]+acc2[0],acc1[1]+acc2[1])))

# 8. find  the top 10 airports based on delays
#    in 7 we already have 'airportAvgDelay', we just need to sort it in descending order
airportAvgDelay.sortBy(lambda x:-x[1])

# 9. 
#    load and parse the airports.csv file
import csv
from StringIO import StringIO
def split(line):
    reader = csv.reader(StringIO(line))
    return reader.next()
def notHeader(row):
    return "Description" not in row

airports=sc.textFile(airportsPath).filter(notHeader).map(split)
#    build a map for 'airports'
airportLookup = airports.collectAsMap()
airportAvgDelay.map(lambda x: (airportLookup[x[0]],x[1]))
#    broadcast variable 'airportLookup'
airportBC = sc.broadcast(airportLookup)
#    so we can use this broadcast variable 
airportAvgDelay.map(lambda x: (airportBC.value[x[0]],x[1]))


#--------------------------------------------------------------------------------
# airlines
airlines=sc.textFile(airlinesPath)
# show RDD path
print airlines
# Using the collect operation, you can view the full dataset
airlines.collect()
# Get the first lines
airlines.first()
# Get the first 10 lines
airlines.take(10)
# Count 
airlines.count()
# filter the header out 
airlinesWoHeader = airlines.filter(lambda x:"Description" not in x)
#
print airlinesWoHeader
airlinesWoHeader.take(10)
# parse each row into a list of the values in the row
# airlinesParsed is a new RDD created
# airlinesParsed is a list instead of (airlines)a string
airlinesParsed=airlinesWoHeader.map(lambda x:x.split(",")).take(10)
airlinesParsed
airlines.map(len).take(10)

# user define function
def notHeader(row):
    return "Description" not in row
# output
airlinesNoHead=airlines.filter(notHeader)



#--------------------------------------------------------------------------------
# spark streaming exemple
#--------------------------------------------------------------------------------
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if__name__ == "__main__":
    
    sc = SparkContext(appName="StreamingErrorCount")
    ssc = StreamingContext(sc, 1)
    
    ssc.checkpoint("hdfs:///user/")
    lines = ssc.socketTextStream(sys.argv[1],int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .filter(lambda word:"ERROR" in word)\
                  .map(lambda word: (word,1))\
                  .reduceByKey(lambda a, b: a+b)
    
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
    





