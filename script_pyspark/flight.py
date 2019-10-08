flightsPath="file:///Users/xiaoxiao/Documents/BigData/udemy_sample/cours/Spark/FlightsData/flights.csv"
airlinesPath="file:///Users/xiaoxiao/Documents/BigData/udemy_sample/cours/Spark/FlightsData/airlines.csv"
airportsPath="file:///Users/xiaoxiao/Documents/BigData/udemy_sample/cours/Spark/FlightsData/airports.csv"

# each row was read in a single string
flights=sc.textFile(flightsPath)

# split RDD flights, return a new RDD with a list
#flightsParsed=flights.map(lambda x: x.split(','))
from datetime import datetime
from collections import namedtuple

fields   = ('date', 'airline', 'flightnum', 'origin', 'dest', 'dep',
            'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance')
Flight   = namedtuple('Flight', fields, verbose=True)

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

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

#airlines=sc.textFile(airlinesPath)

fields2   = ('code', 'description')
Airlines   = namedtuple('airlines', fields2, verbose=True)

def notHeader(row):
    return "Description" not in row

def parse2(row):
    return Airlines(*row[:2])
airlines=sc.textFile(airlinesPath).filter(notHeader)
airlinesParsed=airlines.map(lambda x: x.split(',')).map(parse2)

# change string to int before compare or filter
flightFilter6=flightsParsed.map(lambda x:float(x[6])<10)

flightFilter6filter=flightsParsed.filter(lambda x:float(x[6])<10)