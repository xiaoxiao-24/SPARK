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