// random dataframe
// -----------------------------
// add a column
val pay = Seq((0,"John","man",3000,4),(1,"Lucie","woman",2500,5)).toDF("id","name","sex","salary","nb_month")


// add new column total for the salary

// method 1
pay.withColumn("total",'salary*'nb_month).show

// method 2
pay.createOrReplaceTempView("payment")
sql("select *, salary*nb_month as total from payment").show


// new york city dataset
// -----------------------------

val nytaxi = spark.read.option("header","true").csv("/user/xiaoxiao/nyctaxisub.csv")
nytaxi.printSchema

import org.apache.spark.sql.functions._
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)
val nyTaxi = nytaxi.withColumn("TripTimeInSecs", toInt(nytaxi("trip_time_in_secs"))).withColumn("TripDistance",toDouble(nytaxi("trip_distance")))

import org.apache.spark.sql.types._
val nyTaxiTime = nytaxi.withColumn("DropOffTime",unix_timestamp($"dropoff_datetime", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"))

nyTaxiTime.createOrReplaceTempView("nytaxi_view")
sql("select distinct year(DropOffTime) from nytaxi_view").show
sql("select distinct month(DropOffTime) from nytaxi_view").show
sql("select distinct concat(year(DropOffTime),month(DropOffTime)) from nytaxi_view").show
sql("select distinct case when cast(month(DropOffTime) as int)<10 then concat(year(DropOffTime),'0',month(DropOffTime)) else concat(year(DropOffTime),month(DropOffTime)) end from nytaxi_view").show

// get the number of vendors
nytaxi.agg(countDistinct("vendor_id")).show()
// list the vendors
nyTaxi.createOrReplaceTempView("nytaxi_view")
sql("select distinct vendor_id from nytaxi_view").show

sql("describe nytaxi_view")
sql("select trip_distance from nytaxi_view").show(10)
sql("select max(TripDistance), min(TripDistance), avg(TripDistance) from nytaxi_view").show
sql("select max(TripTimeInSecs), min(TripTimeInSecs), avg(TripTimeInSecs) from nytaxi_view").show