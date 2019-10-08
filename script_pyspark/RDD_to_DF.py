
# get text file to RDD
rdd_age = sc.textFile("/user/amy_ds/sample_dataset/age.txt")                                   
rdd_height = sc.textFile("/user/amy_ds/sample_dataset/height.txt")            
                 
rdd_age                                                                                        
# >>> /user/amy_ds/sample_dataset/age.txt MapPartitionsRDD[19] at textFile at NativeMethodAccessorImpl.java:0                                                                                               
rdd_height                                                                                     
# >>> /user/amy_ds/sample_dataset/height.txt MapPartitionsRDD[21] at textFile at NativeMethodAccessorImpl.java:0                                                                                            

# get tuple RDD
df_age = rdd_age.map(lambda l:l.split(",")).map(lambda a:(a[0],a[1]))                          
df_age                                                                                         
# >>> PythonRDD[22] at RDD at PythonRDD.scala:49                                                         
df_age.collect()                                                                               
# >>> [(u'Alice', u'2'), (u'Bob', u'5')]                                                                 

df_height = rdd_height.map(lambda l:l.split(",")).map(lambda a:(a[0],a[1]))                    
df_height                                                                                      
# >>> PythonRDD[24] at RDD at PythonRDD.scala:49     
df_height.collect()                                                                            
# >>> [(u'Tom', u'80'), (u'Bob', u'85')]  

# create RDD with schema Row type
Person = Row('name','age') 
df1 = df_age.map(lambda r:Person(*r))                                                          
df1                                                                                            
# >>> PythonRDD[23] at RDD at PythonRDD.scala:49                                                         
df1.collect()                                                                                  
# >>> [Row(name=u'Alice', age=u'2'), Row(name=u'Bob', age=u'5')]                                         
                                                                                                               
Height = Row('name','height')                                                           
df2 = df_height.map(lambda r:Height(*r))                                                       
df2                                                                                            
# >>> PythonRDD[25] at RDD at PythonRDD.scala:49                                                         
df2.collect()                                                                                  
# >>> [Row(name=u'Tom', height=u'80'), Row(name=u'Bob', height=u'85')]                                   

# create DataFrame                             
df3 = spark.createDataFrame(df1)                                                               
df3                                                                                            
# >>> DataFrame[name: string, age: string]   
df3.select("age","name").collect()                                                             
# >>> [Row(age=u'2', name=u'Alice'), Row(age=u'5', name=u'Bob')]                                                                                         
                                                            
df5 = df1.toDF()                                                                               
df5                                                                                            
# >>> DataFrame[name: string, age: string]                                                               
df5.select("age","name").collect()                                                             
# >>> [Row(age=u'2', name=u'Alice'), Row(age=u'5', name=u'Bob')]                                         

df4 = spark.createDataFrame(df2)                                                               
df4                                                                                            
# >>> DataFrame[name: string, height: string]                                                            
df4.select("height","name").collect()                                                          
# >>> [Row(height=u'80', name=u'Tom'), Row(height=u'85', name=u'Bob')]                                   

df6 = df2.toDF()                                                                               
df6.select("height","name").collect()                                                          
# >>> [Row(height=u'80', name=u'Tom'), Row(height=u'85', name=u'Bob')]                                   
                                                   
# crossJoin                                                                                                 
df3.crossJoin(df4.select("height")).collect()                                                  
# >>> [Row(name=u'Alice', age=u'2', height=u'80'), Row(name=u'Alice', age=u'2', height=u'85'), Row(name=u'Bob', age=u'5', height=u'80'), Row(name=u'Bob', age=u'5', height=u'85')]                          
# join                                                 
df3.join(df4, df3.name==df4.name, 'inner').drop(df4.name).collect()                            
# >>> [Row(name=u'Bob', age=u'5', height=u'85')]
