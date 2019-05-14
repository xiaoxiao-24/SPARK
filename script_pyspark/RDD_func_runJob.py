# runJob(rdd, partitionFunc, partitions=None, allowLocal=False)
# Executes the given partitionFunc on the specified set of partitions, returning the result as an array of elements.

myRDD = sc.parallelize(range(6),3) # create an RDD, partitioned into 3 partitions

myRDD.collect()                                                                                
>>> [0, 1, 2, 3, 4, 5]

sc.runJob(myRDD, lambda part:[x*x for x in part], [0], True) # partiton 1                                  
>>> [0, 1]                                                                                             
sc.runJob(myRDD, lambda part:[x*x for x in part], [1], True) # partiton 2                                  
>>> [4, 9]                                                                                             
sc.runJob(myRDD, lambda part:[x*x for x in part], [2], True) # partiton 3                                    
>>> [16, 25]  
sc.runJob(myRDD, lambda part:[x*x for x in part], [0,2], True) # partiton 1 and 3                                   
>>> [0, 1, 16, 25]                                                                                     
sc.runJob(myRDD, lambda part:[x*x for x in part], [0,1], True)                                 
>>> [0, 1, 4, 9]  
sc.runJob(myRDD, lambda part:[x*x for x in part], [1,2], True)                                 
>>> [4, 9, 16, 25]     
sc.runJob(myRDD, lambda part:[x*x for x in part], [0,1,2], True)                               
>>> [0, 1, 4, 9, 16, 25]   