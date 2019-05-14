# countByKey

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 3), ("a", 3)])                                 
rdd.countByKey()                                                                               
>>> defaultdict(<type 'int'>, {'a': 3, 'b': 1})                                                        
rdd.countByKey().items()                                                                       
>>> [('a', 3), ('b', 1)] 

# countByValue
sc.parallelize([1, 2, 1, 2, 2, 1, 1, 1, 1], 3).countByValue().items()                              
>>> [(1, 6), (2, 3)]  
