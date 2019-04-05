#create a parallelized collection
data = [1,2,3,4,5]
distData = sc.parallelize(data)

#get the number of partitions of a RDD
distData.getNumPartitions()

#Once created, the distributed dataset (distData) can be operated on in parallel.
#par exemple, add up the elements of the list (distData)
distData.reduce(lambda a,b:a+b)

#read a file(external datasets)
#the file will be read as a collection of lines
distFile = sc.textFile("C:\\Users\\Xiaoxiao\\Documents\\SVN_IP_mfps.txt")
#once added, we can do dataset operations
#for example, add up the sizes of all the lines by using 'map' and 'reduce' operations
distFile.map(lambda s:len(s)).reduce(lambda a,b:a+b)
#print each line in file
from __future__ import print_function
distFile.foreach(print)
