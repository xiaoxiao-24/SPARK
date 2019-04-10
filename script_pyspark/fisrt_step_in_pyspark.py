#create a parallelized collection
data = [1,2,3,4,5]
distData = sc.parallelize(data)

#get the number of partitions of a RDD
distData.getNumPartitions()

#Once created, the distributed dataset (distData) can be operated on in parallel.
#par exemple, add up the elements of the list (distData)
distData.reduce(lambda a,b:a+b)

#read a file(external datasets),count line length
#-------------------------------------------------
# 1. the file will be read as a Collection of Lines
lines = sc.textFile("C:\\Users\\Xiaoxiao\\Documents\\SVN_IP_mfps.txt")
# 2. get length of each line
lineLengths = lines.map(lambda a:len(a))
#once added, we can do dataset operations
# 3. get total length of file 
# for example, add up the sizes of all the lines by using 'map' and 'reduce' operations
totalLenghth = lineLengths.reduce(lambda a,b:a+b)
# 4. print each line in file
from __future__ import print_function
lines.foreach(print)

#define and call a function
#-------------------------------------------------
def WordCount(s):
	words = s.split(" ")
	return len(words)

file = sc.textFile("C:\\Users\\Xiaoxiao\\Documents\\UnderstandingClosures.txt")
nb_words = file.map(WordCount)