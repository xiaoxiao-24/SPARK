y = sc.parallelize([("m",2),("n",3),("n",4)]) 

def to_list(a):                                                                                     
	return [a]                                                                                      
                                                                                                     
def append(a,b):                                                                                    
	a.append(b)                                                                                     
	return a                                                                                        
                                                                                                   
def extend(a,b):                                                                                    
	a.extend(b)                                                                                     
	return a  
	
y.combineByKey(to_list, append, extend).collect()                                                   
# [('m', [2]), ('n', [3, 4])]                            

'''

In this example, the keys are ‘m’ and ‘n’. The values are ints. The task is to combine the ints to a list, for each key.

The first function is createCombiner — which turn a value in the original key-value pair, to a combined type. In this example, the combined type is a list. This function will takes an int and returns a list.

The second function is mergeValue — which adds a new int into the list. This is to combine value with combiners. — thus uses append.

The third function is mergeCombiners — which merges two combiners. Thus in this example is to merge two lists  — thus uses extend.
 
'''