// ---------------------------
// CSV to DataFrame
// ---------------------------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

// 1. create schema
val customSchema = StructType(Seq(
StructField("userId",StringType,true),
StructField("color",StringType,true),
StructField("count",IntegerType,true)))

// 2. import CSV
val resultAsACsvFormat = spark.read.schema(customSchema).option("delimiter",",").csv("C:\\Users\\Xiaoxiao\\Documents\\Git\\SPARK\\script_scala\\Comparison_RDD_DF_DS\\testevent.csv")

// 3. group
val finalResult = resultAsACsvFormat.groupBy("color").sum("count")

// 4. output
val outputDF = finalResult.collect()