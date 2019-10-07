// UDF string


val dataset = Seq((0,"heLLo"),(1,"worLd")).toDF("id","text")

// import library for UDF
import org.apache.spark.sql.functions.udf

// create a UDF: upper class <step 1>
val upper: String => String = _.toUpperCase
// create a UDF <step 2>
val upperUDF = udf(upper)
// use the UDF add a new column which upper the 'text' column
dataset.withColumn("upper",upperUDF('text)).show

// create a UDF: lower class
val lower: String => String = _.toLowerCase
val UDF2Lower = udf(lower)
dataset.withColumn("lower",UDF2Lower('text)).show

// create a UDF: substring
val substr: String => String = _.substring(1,3)
val UDF3substr = udf(substr)
dataset.withColumn("substr",UDF3substr('text)).show

// create a UDF, id = id+1
