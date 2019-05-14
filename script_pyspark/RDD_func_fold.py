# fold(zeroValue, op)
# Aggregate the elements of each partition, and then the results for all the partitions, using a given associative function and a neutral “zero value.”

# The function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation; however, it should not modify t2.

sc.parallelize([1, 2, 3, 4, 5],3).glom().collect()[0]                                          
[1]                                                                                                
sc.parallelize([1, 2, 3, 4, 5],3).glom().collect()[1]                                          
[2, 3]                                                                                             
sc.parallelize([1, 2, 3, 4, 5],3).glom().collect()[2]                                          
[4, 5]   

from operator import add 
sc.parallelize([1, 2, 3, 4, 5],3).fold(1, add)                                                 
19  # (1 + 1) + (1 + 2 + 3) + (1 + 4 + 5) + 1
sc.parallelize([1, 2, 3, 4, 5],3).fold(2, add)                                                 
23                                                                                                                                                                                                 
sc.parallelize([1, 2, 3, 4, 5],3).fold(3, add)                                                 
27                                                                                                 
sc.parallelize([1, 2, 3, 4, 5],3).fold(4, add)                                                 
31                                                                                                 
sc.parallelize([1, 2, 3, 4, 5],3).fold(5, add)                                                 
35 # (5 + 1) + (5 + 2 + 3) + (5 + 4 + 5) + 5                                                                                                
sc.parallelize([1, 2, 3, 4, 5],3).fold(6, add)                                                 
39                                                                                                 
sc.parallelize([1, 2, 3, 4, 5],3).fold(7, add)                                                 
43                                                                                                 
sc.parallelize([1, 2, 3, 4, 5],3).fold(8, add)                                                 
47                                                 
      
from operator import mul                                           
sc.parallelize([1, 2, 3, 4, 5],3).fold(0, mul)                                                 
0                                                                                                   
sc.parallelize([1, 2, 3, 4, 5],3).fold(1, mul)                                                 
120 # (1 * 1) * (1 * 2 * 3) * (1 * 4 * 5) * 1                                                                                               
sc.parallelize([1, 2, 3, 4, 5],3).fold(2, mul)                                                 
1920 # (2 * 1) * (2 * 2 * 3) * (2 * 4 * 5) * 2                                                                                                 
sc.parallelize([1, 2, 3, 4, 5],3).fold(3, mul)                                                 
9720                                                                                               
sc.parallelize([1, 2, 3, 4, 5],3).fold(4, mul)                                                 
30720    