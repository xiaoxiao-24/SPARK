// exemple: word count 
// author: xiaoxiao

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val txtFile = "README.md"
val txtData = sc.textFile(txtFile)
txtData.cache()

val wcData = txtData.flatMap(l => l.split(" ")).map(word =>(word, 1)).reduceByKey(_+_)

wcData.collect().foreach(println)
