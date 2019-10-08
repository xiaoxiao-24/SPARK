#objective: count 'a' and 'b' in file
#the following 2 lines needed for lancer the script with python program
#lancer a pyspark script with spark: spark-submit firstapp.py
from pyspark import SparkContext
sc = SparkContext("local","first app")

logFile = "file:///home/test.txt"
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print "Lines with a: %i, lines with b: %i" % (numAs,numBs)