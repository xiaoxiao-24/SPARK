// check data in hive: neumanndb

sql("select * from city").show(5)

sql("select * from imsinormalized where imsi = 420040148271738").show

// list all the iphone
sql("select distinct handset,substring(handset,13) from handset where handset like '%Apple%'").show

// list all the phone brands and the numbre of phone he gots
sql("select type,count(1) from handset group by type ").show

// la distribution de smartphone vs no-smartphone
sql("select h.smartphone,count(1) from handset h join profile p on h.handset=p.handset group by h.smartphone").show

+----------+--------+                                                           
|smartphone|count(1)|
+----------+--------+
|      true|   22356|
|     false|   27643|
+----------+--------+

sql("select max(averagemonthlybill),min(averagemonthlybill),avg(averagemonthlybill) from valuepostpaid").show
+-----------------------+-----------------------+-----------------------+
|max(averagemonthlybill)|min(averagemonthlybill)|avg(averagemonthlybill)|
+-----------------------+-----------------------+-----------------------+
|                4540.53|               -1322.13|     157.44464546454705|
+-----------------------+-----------------------+-----------------------+

sql("select max(rechargeamount),min(rechargeamount),avg(rechargeamount) from valueprepaid").show
+-------------------+-------------------+-------------------+
|max(rechargeamount)|min(rechargeamount)|avg(rechargeamount)|
+-------------------+-------------------+-------------------+
|           21320.04|                0.0|  569.6478869999308|
+-------------------+-------------------+-------------------+

sql("select count(distinct type) from packageplan").show()
sql("select distinct type from packageplan").show()
sql("select type,count(distinct packageplan) from packageplan group by type").show()

// pour le client (420040100212496): la distribution de ses calls dans la journée
sql("select h.type,sum(answeredcalls) from behaviorcalls b join hourofday h on b.hourofday=h.hourofday where imsi = '420040100212496' group by h.type").show(false)
+--------------+------------------+                                             
|type          |sum(answeredcalls)|
+--------------+------------------+
|Evening       |1019              |
|Morning       |733               |
|Afternoon     |1280              |
|Night         |779               |
|After Midnight|407               |
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
|CostLevel|count(1)|
+---------+--------+
|     0-50|   50706|
|   50-100|    4274|
|     100+|      19|
+---------+--------+

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
+-----------+--------+                                                          
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
+-----------+--------+

// --------------------------------------------------------------------
// DataFrame to CSV
// --------------------------------------------------------------------

// write to HDFS
val ImsiDF = sql("select * from imsi")
import org.apache.spark.sql.SaveMode

ImsiDF.write.format("csv").mode(SaveMode.Overwrite).option("header", "true").option("sep",",").csv("/user/xiaoxiao/neumann_ismi")

ImsiDF.write.format("csv").mode("overwrite").option("header", "true").option("sep",",").save("/user/xiaoxiao/neumann_ismi2")

ImsiDF.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("/user/xiaoxiao/neumann_ismi2")


// read the file qu'on a 'write'
val ImsiDFRead = spark.read.option("header", "true").csv("/user/xiaoxiao/neumann_ismi/part-00000-98890a34-e5f6-43a5-bfe5-d16d070ad255-c000.csv.deflate")
ImsiDFRead.show(3)

// read the file on unix line commande
// 1) decompress the deflate file:
hdfs dfs -text /user/xiaoxiao/neumann_ismi/part-00000-98890a34-e5f6-43a5-bfe5-d16d070ad255-c000.csv.deflate | hdfs dfs -put - imsi_df
// 2) read decompressed file
hdfs dfs -tail /user/xiaoxiao/imsi_df