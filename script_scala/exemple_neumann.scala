// check data in hive: neumanndb

sql("select * from city").show(5)

sql("select * from imsinormalized where imsi = 420040148271738").show

// list all the iphone
sql("select distinct handset,substring(handset,13) from handset where handset like '%Apple%'").show

// list all the phone brands and the numbre of phone he gots
sql("select type,count(1) from handset group by type ").show

// la distribution de smartphone vs no-smartphone
sql("select h.smartphone,count(1) from handset h join profile p on h.handset=p.handset group by h.smartphone").show
/*
+----------+--------+                                                           
|smartphone|count(1)|
+----------+--------+
|      true|   22356|
|     false|   27643|
+----------+--------+*/

sql("select max(averagemonthlybill),min(averagemonthlybill),avg(averagemonthlybill) from valuepostpaid").show
/*+-----------------------+-----------------------+-----------------------+
|max(averagemonthlybill)|min(averagemonthlybill)|avg(averagemonthlybill)|
+-----------------------+-----------------------+-----------------------+
|                4540.53|               -1322.13|     157.44464546454705|
+-----------------------+-----------------------+-----------------------+*/

sql("select max(rechargeamount),min(rechargeamount),avg(rechargeamount) from valueprepaid").show
/*+-------------------+-------------------+-------------------+
|max(rechargeamount)|min(rechargeamount)|avg(rechargeamount)|
+-------------------+-------------------+-------------------+
|           21320.04|                0.0|  569.6478869999308|
+-------------------+-------------------+-------------------+*/

sql("select count(distinct type) from packageplan").show()
sql("select distinct type from packageplan").show()
sql("select type,count(distinct packageplan) from packageplan group by type").show()

// pour le client (420040100212496): la distribution de ses calls dans la journée
sql("select h.type,sum(answeredcalls) from behaviorcalls b join hourofday h on b.hourofday=h.hourofday where imsi = '420040100212496' group by h.type").show(false)
<<<<<<< HEAD
/*+--------------+------------------+                                             
=======
+--------------+------------------+                                             
>>>>>>> 96bfb40880462bd1680aed9fe469bcc0ff0c0bcd
|type          |sum(answeredcalls)|
+--------------+------------------+
|Evening       |1019              |
|Morning       |733               |
|Afternoon     |1280              |
|Night         |779               |
|After Midnight|407               |
<<<<<<< HEAD
+--------------+------------------+*/

// marketing cost
sql("select max(market_cost),min(market_cost),avg(market_cost) from marketing").show
/*+----------------+----------------+------------------+
|max(market_cost)|min(market_cost)|  avg(market_cost)|
+----------------+----------------+------------------+
|             125|               0|27.058273786796125|
+----------------+----------------+------------------+*/

sql("select case when market_cost >= 0 and market_cost <= 50 then '0-50' when market_cost >= 51 and market_cost <= 100 then '50-100' else '100+' end as CostLevel,count(1) from marketing group by CostLevel").show
/*+---------+--------+
=======
+--------------+------------------+

// marketing cost
sql("select max(market_cost),min(market_cost),avg(market_cost) from marketing").show
+----------------+----------------+------------------+
|max(market_cost)|min(market_cost)|  avg(market_cost)|
+----------------+----------------+------------------+
|             125|               0|27.058273786796125|
+----------------+----------------+------------------+

sql("select case when market_cost >= 0 and market_cost <= 50 then '0-50' when market_cost >= 51 and market_cost <= 100 then '50-100' else '100+' end as CostLevel,count(1) from marketing group by CostLevel").show
+---------+--------+
>>>>>>> 96bfb40880462bd1680aed9fe469bcc0ff0c0bcd
|CostLevel|count(1)|
+---------+--------+
|     0-50|   50706|
|   50-100|    4274|
|     100+|      19|
<<<<<<< HEAD
+---------+--------+*/
=======
+---------+--------+
>>>>>>> 96bfb40880462bd1680aed9fe469bcc0ff0c0bcd

// une date le capacity d'un site et le vrai connexion
// get the actuel connexion by site
sql("select handsetlocation,count(1) from behaviorcalls where dateofcall like '2011-09-17%' group by handsetlocation order by count(1) desc").show(false)
// compare the actuel connexion with the capacity of site
sql("select s.handsetlocation, capacity, h.actuel_20110917 from site_capacity s join (select handsetlocation,count(1) as actuel_20110917 from behaviorcalls where dateofcall like '2011-09-17%' group by handsetlocation) h on h.handsetlocation=s.handsetlocation").show(false)

sql("select s.handsetlocation, capacity, h.actuel_20110917 from site_capacity s join (select handsetlocation,count(1) as actuel_20110917 from behaviorcalls where dateofcall like '2011-09-17%' group by handsetlocation) h on h.handsetlocation=s.handsetlocation where capacity <= h.actuel_20110917").show(false)



// nombre de détail consommation de cette personne (420040100212496)
sql("select count(1) from behaviorsms where imsi = '420040100212496'").show  // 82
sql("select count(1) from behaviorcalls where imsi = '420040100212496'").show  // 807
sql("select count(1) from behaviordata where imsi = '420040100212496'").show  // 567
sql("select count(1) from datalog where imsi = '420040100212496'").show  // 193

//
sql("select count(1) from behaviorsms where imsi = '420040100212496' and datetime like '2014-08-27%'").show  // 0
sql("select count(1) from behaviorcalls where imsi = '420040100212496' and dateofcall like '2014-08-27%'").show  // 1
sql("select count(1) from behaviordata where imsi = '420040100212496' and datetime like '2014-08-27%'").show  // 0
sql("select count(1) from datalog where imsi = '420040100212496' and datetime like '2014-08-27%'").show  // 88

//
sql("select count(1) from behaviorsms ").show   //1272288
sql("select count(1) from behaviorcalls ").show  //11913342
sql("select count(1) from behaviordata ").show  //4701991
sql("select count(1) from datalog ").show  //3361325

// distribution of phone by brand
sql("select handsettype,count(1) from imsi group by handsettype").show
<<<<<<< HEAD
/*+-----------+--------+                                                          
=======
+-----------+--------+                                                          
>>>>>>> 96bfb40880462bd1680aed9fe469bcc0ff0c0bcd
|handsettype|count(1)|
+-----------+--------+
|      Nokia|   27803|
|       Sony|     447|
|    Alcatel|     125|
| BlackBerry|    1813|
|      Other|    6403|
|    Samsung|    9602|
|        HTC|     226|
|         LG|     251|
|      Apple|    3329|
|           |    5000|
<<<<<<< HEAD
+-----------+--------+*/
=======
+-----------+--------+
>>>>>>> 96bfb40880462bd1680aed9fe469bcc0ff0c0bcd

// --------------------------------------------------------------------
// DataFrame to CSV / JSON
// --------------------------------------------------------------------

// write to HDFS
val ImsiDF = sql("select * from imsi")
import org.apache.spark.sql.SaveMode

ImsiDF.write.format("csv").mode(SaveMode.Overwrite).option("header", "true").option("sep",",").csv("/user/xiaoxiao/neumann_ismi")

ImsiDF.write.format("csv").mode("overwrite").option("header", "true").option("sep",",").save("/user/xiaoxiao/neumann_ismi2")

ImsiDF.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("/user/xiaoxiao/neumann_ismi2")

ImsiDF.write.format("json").mode(SaveMode.Overwrite).option("header", "true").json("/user/xiaoxiao/neumann_ismi_json")

val imsi_churnrate = sql("select * from churn_rate")
import org.apache.spark.sql.SaveMode
imsi_churnrate.write.format("json").mode(SaveMode.Overwrite).option("header", "true").json("/user/xiaoxiao/neumann_churnrate_json")

// read the file qu'on a 'write'
val ImsiDFRead = spark.read.option("header", "true").csv("/user/xiaoxiao/neumann_ismi/part-00000-98890a34-e5f6-43a5-bfe5-d16d070ad255-c000.csv.deflate")
ImsiDFRead.show(3)

/* ----------------------------------------------
    decompresser .deflate file généré par spark
   ---------------------------------------------- */ 

// decompresser the .deflate file on unix line commande

// to CSV
// 1) decompress:
hdfs dfs -text /user/xiaoxiao/neumann_ismi/part-00000-98890a34-e5f6-43a5-bfe5-d16d070ad255-c000.csv.deflate | hdfs dfs -put - imsi_df
// 2) check decompressed file
hdfs dfs -tail /user/xiaoxiao/imsi_df

// to JSON
// 1) decompress:
hdfs dfs -text /user/xiaoxiao/neumann_ismi_json/part-00000-7f62b4e8-8bf8-4c27-9fa0-e57f5604d314-c000.json.deflate | hdfs dfs -put - imsi_json.json
// 2) check decompressed file
hdfs dfs -tail /user/xiaoxiao/imsi_json.json


hdfs dfs -text /user/xiaoxiao/neumann_churnrate_json/part-00000-25af08ce-5e25-4997-bdac-4b9d699a421d-c000.json.deflate | hdfs dfs -put - churnrate.json

// --------------------------------------------------------------------
// correlation
// --------------------------------------------------------------------
val imsi_DF = spark.sql("select * from imsi")

// Correlation
val correlation = imsi_DF.stat.corr("arpu", "totaldatabytes")
println(s"correlation between column totalcallminutes and totalcallcount = $correlation")

// between "Phone" and "totalspent
// transformer column "handsettype":string to an INT column
val imsi_DF_handset = spark.sql("""select case when handsettype='Alcatel' then 10
                                             |when handsettype='HTC' then 9
                                             |when handsettype='LG' then 8
                                             |when handsettype='Sony' then 7
                                             |when handsettype='BlackBerry' then 6
                                             |when handsettype='Apple' then 5
                                             |when handsettype='Other' then 4
                                             |when handsettype='Samsung' then 3
                                             |when handsettype='Nokia' then 2
                                             |else 1 end as Phone,totalspent,arpu from imsi""")
val correlation = imsi_DF_handset.stat.corr("Phone", "arpu")

// between "city" and others
val imsi_DF_city = spark.sql("""select distinct city from imsi """)
val city_DF = imsi_DF_city.withColumn("index",monotonically_increasing_id())
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
val city_DF = imsi_DF_city.withColumn("index",functions.row_number().over(Window.orderBy("city")))
val city_DF = imsi_DF_city.withColumn("index",row_number().over(Window.orderBy("city")))  // or this one
val imsi_city_spend = spark.sql("""select city,
                                          avg(arpu) as arpu,
                                          avg(totalspent) as totalspent
                                          from imsi group by city""")
val cityDF_cor = imsi_city_spend.join(city_DF, "city")
val correlation = cityDF_cor.stat.corr("index", "arpu")

// "category"
val imsi_cat = spark.sql("""select distinct category from imsi """)
val cat_DF = imsi_cat.withColumn("index_cat",functions.row_number().over(Window.orderBy("category")))
val imsi_spend = spark.sql("""select category, city, arpu, totalspent from imsi """)
val cityDF_cor = imsi_spend.join(cat_DF, "category").join(city_DF, "city")
val correlation = cityDF_cor.stat.corr("index_cat", "arpu")

val imsi_DF2 = imsi_DF.join(city_DF,"city").join(cat_DF,"category")
val correlation = cityDF_cor.stat.corr("index_cat", "arpu")

// Covariance
val covariance = imsi_DF.stat.cov("totalcallminutes", "totalcallcount")
println(s"covariance between column totalcallcount and totalcallminutes = $covariance")


// Frequent Items
val dfFrequentCity = imsi_DF.stat.freqItems(Seq("city"))
dfFrequentCity.show()

// sampleBy()
imsi_DF.groupBy("category").count()
val fractionKeyMap = Map("Local" -> 0.01, "Corporate" -> 0.01, "Expat" -> 0.002, "Other" -> 0.02, "" -> 0.01)
// Stratified sample using the fractionKeyMap.
imsi_DF.stat.sampleBy("category", fractionKeyMap, 7L).groupBy("category").count().show()


// quantile
// Approximate Quantile
val quantiles = imsi_DF.stat.approxQuantile("arpu", Array(0, 0.5, 1), 0.25)
println(s"Qauntiles segments = ${quantiles.toSeq}")
// with Spark SQL
sql("select min(arpu), percentile_approx(arpu, 0.25), max(arpu) from imsi").show()


// Bloom Filter
// when training large datasets in a Machine Learning pipeline, use Bloom Filter to take only une partie of the dataset, and use mightContain() to check if a value exists
val handsetBloomFilter = imsi_DF.stat.bloomFilter("handsettype", 500L, 0.1)
println(s"bloom filter contains Nokia = ${handsetBloomFilter.mightContain("Nokia")}")
println(s"bloom filter contains Samsung = ${handsetBloomFilter.mightContain("Samsung")}")

// Count Min Sketch
/*
first parameter = the tag column of dataframe dfTags
second parameter = 10% precision error factor
third parameter = 90% confidence level
fourth parameter = 37 as a random seed */
val handset = imsi_DF.stat.countMinSketch("handsettype", 0.1, 0.9, 37)
val estimatedFrequency = handset.estimateCount("Samsung")   // on estime qu'il y a 9602 Samsung
println(s"Estimated frequency for Samsung = $estimatedFrequency")


// DataFrame new column with User Defined Function (UDF)
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._

val handsetid: (String => Int) = (handsettype: String) => handsettype match {
    case "Nokia"    => 10
    case "Samsung"  => 9
    case "Other"    => 8
    case ""         => 7
    case "Apple"    => 6
    case "BlackBerry"   => 5
    case "Sony"     => 4
    case "LG"       => 3
    case "HTC"      => 2
    case "Alcatel"  => 1
}

val udfhandsetid = udf(handsetid)
val imsi_DF2 = imsi_DF.withColumn("handsetid", udfhandsetid($"handsettype"))
imsi_DF2.show()


