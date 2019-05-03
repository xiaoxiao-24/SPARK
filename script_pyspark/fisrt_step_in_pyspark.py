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
# 1. the file will be read as a Collection of Lines !!!
# each line is an element in the list
lines = sc.textFile("C:\\Users\\Xiaoxiao\\Documents\\SVN_IP_mfps.txt")
lines.collect()
lines.foreach(print)
# ['http://svngdrpi.gdrpi.fr/svn/GDRPI/PROD', '', '172.21.56.50']
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

# --- flatmap() ---
# transform a text file into a list of words
lines_split = lines.map(lambda x: x.split('/'))
lines_split.collect()
# [['http:', '', 'svngdrpi.gdrpi.fr', 'svn', 'GDRPI', 'PROD'], [''], ['172.21.56.50']]
lines_flat = lines.flatMap(lambda line: line.split('/'))
lines_flat.collect()
# ['http:', '', 'svngdrpi.gdrpi.fr', 'svn', 'GDRPI', 'PROD', '', '172.21.56.50']
lines_flat2 = lines_flat.flatMap(lambda line: line.split('.'))
lines_flat2.collect()
# ['http:', '', 'svngdrpi', 'gdrpi', 'fr', 'svn', 'GDRPI', 'PROD', '', '172', '21', '56', '50']


# filter()
lines_filter = lines_flat.filter(lambda x:'r' in x)
lines_filter.collect()
# ['svngdrpi.gdrpi.fr']


# create a DataFrame from a JSON file
df = spark.read.json("C:\\Users\\Xiaoxiao\\Documents\\people.json")