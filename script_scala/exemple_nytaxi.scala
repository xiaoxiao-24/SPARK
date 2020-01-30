// -----------------------------
// random dataframe
// -----------------------------
// add a column
val pay = Seq((0,"John","man",3000,4),(1,"Lucie","woman",2500,5)).toDF("id","name","sex","salary","nb_month")

pay.filter(col("id")===1).show
pay.filter(col("id")===1).collect()

// add new column total for the salary

// method 1
pay.withColumn("total",'salary*'nb_month).show

// method 2
pay.createOrReplaceTempView("payment")
sql("select *, salary*nb_month as total from payment").show


// ---------------------------------------------
//  new york city dataframe
// ---------------------------------------------

val nytaxi = spark.read.option("header","true").csv("/user/xiaoxiao/nyctaxisub.csv")
nytaxi.printSchema

import org.apache.spark.sql.functions._
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)

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


val nytaxi = spark.read.option("header","true").csv("/user/xiaoxiao/nyctaxisub.csv")
val nyTaxi = nytaxi.withColumn("TripDistance",toDouble(nytaxi("trip_distance"))).withColumn("TripTimeInSecs", toInt(nytaxi("trip_time_in_secs"))).withColumn("Rate",toInt(nytaxi("rate_code"))).withColumn("NbPassenger",toInt(nytaxi("passenger_count"))).withColumn("DropOffTime",unix_timestamp($"dropoff_datetime", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp")).withColumn("PickUpTime",unix_timestamp($"pickup_datetime", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"))


nyTaxi.select("TripDistance","TripTimeInSecs","Rate","NbPassenger","PickUpTime","DropOffTime").show(5)

sql("select dayofweek('2019-10-10')").show // monday = 2 etc
sql("select distinct vendor_id from nytaxi_view").show
sql("describe nytaxi_view")
sql("select trip_distance from nytaxi_view").show(10)
sql("select max(TripDistance), min(TripDistance), avg(TripDistance) from nytaxi_view").show
sql("select max(TripTimeInSecs), min(TripTimeInSecs), avg(TripTimeInSecs) from nytaxi_view").show

// get the total rides by the day of week
sql("select dayofweek(PickUpTime),count(1) from nytaxi_view group by dayofweek(PickUpTime) order by dayofweek(PickUpTime)").show

// get the total rides by the hour of day
sql("select hour(PickUpTime),count(1) from nytaxi_view group by hour(PickUpTime) order by hour(PickUpTime)").show

// get avg trip time in minutes
sql("select max(TripTimeInSecs)/60,min(TripTimeInSecs)/60,avg(TripTimeInSecs)/60 from nytaxi_view").show

// count all: 249999
sql("select count(1) from nytaxi_view")


// ---------------------------------------------
//  new york city dataset
// ---------------------------------------------

import org.apache.spark.sql.types._
val customSchema = StructType(Seq(
StructField("Id",StringType,true),
StructField("rev",StringType,true),
StructField("dropoff_datetime",TimestampType,true),
StructField("dropoff_latitude",DoubleType,true),
StructField("dropoff_longitude",DoubleType,true),
StructField("hack_license",StringType,true),
StructField("medallion",StringType,true),
StructField("passenger_count",IntegerType,true),
StructField("pickup_datetime",TimestampType,true),
StructField("pickup_latitude",DoubleType,true),
StructField("pickup_longitude",DoubleType,true),
StructField("rate_code",IntegerType,true),
StructField("store_and_fwd_flag",StringType,true),
StructField("trip_distance",DoubleType,true),
StructField("trip_time_in_secs",IntegerType,true),
StructField("vendor_id",StringType,true)
)
)

import java.sql.Timestamp
case class NYCTaxi(Id: String, rev: String, dropoff_datetime: Timestamp, dropoff_latitude: Double, dropoff_longitude: Double, hack_license: String, medallion: String, passenger_count: Int, pickup_datetime: Timestamp, pickup_latitude: Double, pickup_longitude: Double, rate_code: Int, store_and_fwd_flag: String, trip_distance: Double, trip_time_in_secs: Int, vendor_id: String)

val NyTaxiDS = spark.read.schema(customSchema).option("header","true").option("delimiter",",").csv("/user/xiaoxiao/nyctaxisub.csv").as[NYCTaxi]

// operations sur dataset columns
NyTaxiDS.agg(avg(NyTaxiDS("trip_distance"))).show()


NyTaxiDS.createOrReplaceTempView("NyTaxiDS_view")

// get vitesse du trip
sql("select pickup_datetime,dropoff_datetime,trip_time_in_secs/60,trip_distance,trip_distance/(trip_time_in_secs/3600) as vitesse from NyTaxiDS_view").show

// calcul la distance with latitute/longitude (power function: pow(3,2) = 3^2)
sql("select trip_distance, 2 * 3961 * asin(sqrt(pow((sin(radians((pickup_latitude - dropoff_latitude) / 2))),2) + pow(cos(radians(dropoff_latitude)) * cos(radians(pickup_latitude)) * (sin(radians((pickup_longitude - dropoff_longitude) / 2))),2))) as distance from NyTaxiDS_view").show

// get error lignes: the 2-points_distance > trip_distance
// 4882 lignes, 2% du total
sql("select count(1) from NyTaxiDS_view where 2 * 3961 * asin(sqrt(pow((sin(radians((pickup_latitude - dropoff_latitude) / 2))),2) + pow(cos(radians(dropoff_latitude)) * cos(radians(pickup_latitude)) * (sin(radians((pickup_longitude - dropoff_longitude) / 2))),2))) >= trip_distance").show