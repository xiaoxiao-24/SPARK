// RDD
// create RDD from file

// 1.read from csv
val events = spark.sparkContext.textFile("C:\\Users\\Xiaoxiao\\Documents\\Git\\SPARK\\script_scala\\Comparison_RDD_DF_DS\\testevent.csv")

// 2. split lines
val splittedEvents = events.map(_.split(","))
// or:
val splittedEvents = events.map(event => event.split(","))

// 3. create pair event
val pairEvents = splittedEvents.map(splittedEvents => (splittedEvents(1),splittedEvents(2).toInt))

// 4. group result
val finalEvents = pairEvents.reduceByKey(_+_)

// 5. output
val output = finalEvents.collect()



// we can put step 2-5 together
val output1 = events.map(_.split(",")).map(splittedEvents => (splittedEvents(1),splittedEvents(2).toInt)).reduceByKey(_+_).collect()