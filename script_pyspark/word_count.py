# word count

lines = sc.textFile("D:\\xiaoxiao\\text.txt")
lines_split = lines.flatMap(lambda line:line.split(' '))
words = lines_split.map(lambda word:(word,1))
words_count = words.reduceByKey(lambda a,b:a+b)
words_count_sort = words_count.sortBy(lambda a:a[1]) # sort ascendant
words_count_sort = words_count.sortBy(lambda a:-a[1]) # sort descendant

# put all together
words_count = sc.textFile("D:\\xiaoxiao\\text.txt")\
				.flatMap(lambda line:line.split(' '))\
				.map(lambda word:(word,1))\
				.reduceByKey(lambda a,b:a+b)

# afficher tous les (word,count) pairs
for (word, count) in words_count.collect():
	print(word, count)

from pyspark.sql.types import *
string_schema = "word count"
StrSchema = string_schema.split(" ")
schema = StructType([StructField(StrSchema[0],StringType(),True),StructField(StrSchema[1],IntegerType(),True)])
words_DF = sqlContext.createDataFrame(words_count,schema) # for spark 1.0
words_DF = spark.createDataFrame(words_count,schema) # for spark 2.0