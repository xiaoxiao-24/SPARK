// Dataset
// create Dataset from file

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

// 1. create schema
val customSchema = StructType(Seq(
StructField("userId",StringType,true),
StructField("color",StringType,true),
StructField("count",IntegerType,true)
)
)

// 2. create a class to link data to class
case class AbTest(userId: String, color: String, count: Integer)

// 3. import CSV
val resultDS = spark.read.schema(customSchema).option("delimiter",",").csv("C:\\Users\\Xiaoxiao\\Documents\\Git\\SPARK\\script_scala\\Comparison_RDD_DF_DS\\testevent.csv").as[AbTest]
// pas obligatoirement besoin de case class???
val resultDS = spark.read.schema(customSchema).option("delimiter",",").csv("/user/xiaoxiao/testevent.csv")

// 4. group 
val finalResultDS = resultDS.groupBy("color").sum("count")

// 5. output
val outputDS = finalResultDS.collect()
