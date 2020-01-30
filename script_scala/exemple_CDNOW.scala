// CDNOW dataset containes the entire purchase history up to the end of June 1998 of the cohort of 23,570 individuals who made their first-ever purchase at CDNOW in the first quarter of 1997.

/*
CustID = CDNOW_master(:,1); % customer id
Date = CDNOW_master(:,2); % transaction date
Quant = CDNOW_master(:,3); % number of CDs purchased
Spend = CDNOW_master(:,4); % dollar value (excl. S&H)
*/


// path in HDFS
val path = "/user/xiaoxiao/CDNOW/CDNOW-master-clean.csv"

import org.apache.spark.sql.types._
val schema = StructType(Seq(
    StructField("CustID",IntegerType,true),
    StructField("Date",IntegerType,true),  
    StructField("Quant",IntegerType,true),
    StructField("Spend",FloatType,true)
    )
)

val CDNOW_DF = spark.read.schema(schema).option("header","false").option("delimiter",",").csv(path)

val col_id = CDNOW_DF.describe("CustID")
col_id.show()

val col_date = CDNOW_DF.describe("Date")
col_date.show()

val col_quant = CDNOW_DF.describe("Quant")
col_quant.show()

val col_spend = CDNOW_DF.describe("Spend")
col_spend.show()

CDNOW_DF.select("CustID").distinct().count()