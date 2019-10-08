# python 3.0 & spark 2.0
"""
med_load = spark.sql('show tables')
med_load = spark.sql('SELECT * FROM dm_medic_avec_composition')
med_load = spark.sql('SELECT * FROM oppchain_esante.dm_medic_avec_composition')
med_composition = sqlContext.table("oppchain_esante.dm_medic_avec_composition")
"""

# Python version 2.7.5 & spark 1.6.3
# read HIVE table, get a column and count the words
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
hive_context.sql("show databases").show()
hive_context.sql("use oppchain_esante")
med_compo_load = hive_context.sql('SELECT * FROM dm_medic_avec_composition')

med_substance = hive_context.sql('SELECT denomination_substance FROM dm_medic_avec_composition')

from operator import add

counts = med_substance.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
output = counts.collect()


counts2 = med_substance.map(lambda x: (x, 1)).reduceByKey(add)
output2 = counts2.collect()

for (word, count) in output2:
    if count >= 100:
        print("%s: %i" % (word, count))
    else:
        pass 

# transform a DataFrame(list) to RDD
rdd_med_sub = med_substance.rdd.map(list)
rdd_med_sub.sortByKey().take(10)

output2.sort().take(10) #err
    
# check if an objet est un RDD ou un DataFrame
from pyspark.sql import DataFrame
from pyspark.rdd import RDD

def foo(x):
    if isinstance(x, RDD):
        return "RDD"
    if isinstance(x, DataFrame):
        return "DataFrame"
    
# check type of an objet
type(output2)

