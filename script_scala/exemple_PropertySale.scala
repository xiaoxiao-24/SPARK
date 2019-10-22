// Allegheny County Property Sale Transactions
// data source: https://catalog.data.gov/dataset/allegheny-county-property-sale-transactions


val path = "/Users/xiaoxiaorey/Documents/Codes/sample-data/PropertySale/AlleghenyPropertySale.csv"
// hdfs:  /user/xiaoxiao/AlleghenyPropertySale.csv

// dataframe
val PropertySale = spark.read.option("header","true").option("delimiter",",").csv(path)

// numbre of properties on sale by city
PropertySale.groupBy("PROPERTYCITY").count().show()

// dataset
import org.apache.spark.sql.types._
val customSchema = StructType(Seq(
    StructField("ParId",StringType,true),
    StructField("HouseNum",IntegerType,true),
    StructField("Fraction",StringType,true),
    StructField("AddressDir",StringType,true),
    StructField("Street",StringType,true),
    StructField("AddressSuf",StringType,true),
    StructField("UnitDesc",StringType,true),
    StructField("UnitNo",StringType,true),
    StructField("City",StringType,true),
    StructField("State",StringType,true),
    StructField("ZipCode",StringType,true),
    StructField("SchoolCode",IntegerType,true),
    StructField("SchoolDesc",StringType,true),
    StructField("MuniCode",IntegerType,true),
    StructField("MuniDesc",StringType,true),
    StructField("RecodeDate",TimestampType,true),
    StructField("SaleDate",TimestampType,true),
    StructField("Price",IntegerType,true),
    StructField("DeedBook",IntegerType,true),
    StructField("DeedPage",IntegerType,true),
    StructField("SaleCode",StringType,true),
    StructField("SaleDesc",StringType,true),
    StructField("Instrtyp",StringType,true),
    StructField("InstrtypDesc",StringType,true)
    )
)

import java.sql.Timestamp
case class PropertySaleClass(
    ParId: String, 
    HouseNum: Int,
    Fraction: String,
    AddressDir: String,
    Street: String,
    AddressSuf: String,
    UnitDesc: String,
    UnitNo: String,
    City: String,
    State: String,
    ZipCode: String,
    SchoolCode: Int,
    SchoolDesc: String,
    MuniCode: Int,
    MuniDesc: String,
    RecodeDate: Timestamp,
    SaleDate: Timestamp,
    Price: Int,
    DeedBook: Int,
    DeedPage: Int,
    SaleCode: String,
    SaleDesc: String,
    Instrtyp: String,
    InstrtypDesc: String
)

val PropertySaleDS = spark.read.schema(customSchema).option("header","true").option("delimiter",",").csv(path).as[PropertySaleClass]

PropertySaleDS.select("City").distinct().show(false)

System.out.println("=== Sale in City : PITTSBURGH ===")
PropertySaleDS.filter(row => row.City == "PITTSBURGH").show(false)

System.out.println("=== Number of sale descendant by school district ===")
PropertySaleDS.groupBy("SchoolDesc").count().orderBy(desc("count")).show(false)

System.out.println("=== Valid sales ===")
PropertySaleDS.filter(row => row.SaleCode == "0" | row.SaleCode == "U" | row.SaleCode == "UR").show(false)

System.out.println("=== Top 20 most expensive sale ===")
PropertySaleDS.filter(row => row.SaleCode == "0" | row.SaleCode == "U" | row.SaleCode == "UR").orderBy(PropertySaleDS.col("Price").desc).show(false)
//PropertySaleDS.orderBy(desc("Price")).show(false)

System.out.println("=== Top 20 School district (number of sales) ===")
PropertySaleDS.filter(row => row.SaleCode == "0" | row.SaleCode == "U" | row.SaleCode == "UR").groupBy("SchoolDesc").count().orderBy(desc("count")).show(false)

System.out.println("=== Top 20 most expensive School district ===")
PropertySaleDS.filter(row => row.SaleCode == "0" | row.SaleCode == "U" | row.SaleCode == "UR").groupBy("SchoolDesc").avg("Price").orderBy(desc("avg(Price)")).show(false)


// ----------------------------
// create temprary view
// ----------------------------
PropertySaleDS.createOrReplaceTempView("PropertySaleView")

// check how many lignes we have in our dataset
sql("select count(1) from PropertySaleView").show(false)  // 269337

// how many school distrincts exist in Allegheny County and list them by the quantity de property on descendant
sql("select count(distinct SchoolDesc) from PropertySaleView").show(false) //46
sql("select SchoolDesc,count(1) from PropertySaleView group by SchoolDesc order by count(1) desc").show(false)
sql("select SchoolDesc,count(1),avg(Price) from PropertySaleView group by SchoolDesc order by avg(Price) desc").show(false)

// how many cities in Allegheny County who got property on sale and list them by the quantity de property on descendant
sql("select count(distinct City) from PropertySaleView").show(false) // 98
sql("select distinct City from PropertySaleView").show(false)
sql("select City,count(1) from PropertySaleView group by City order by count(1) desc").show

// top 20 most expensive city
sql("select City, avg(Price),count(1) from PropertySaleView where SaleCode in ('0','U','UR') group by City order by avg(Price) desc").show(false)

// get the sale periode (begin, end)
sql("select min(SaleDate),max(SaleDate) from PropertySaleView").show(false)  // 2012 - 2019

// how many municipal there is 
sql("select count(distinct MuniDesc) from PropertySaleView").show(false) // 175

// distribution by the address direction
sql("select AddressDir, count(1) from PropertySaleView group by AddressDir").show(false)

sql("select AddressDir, case when AddressDir = 'N' then 'North' when AddressDir = 'W' then 'West' else 'South' end as Dir from PropertySaleView where AddressDir in ('N','S','W') ").show(false)

sql("select AddressDir, avg(Price), count(1) from PropertySaleView where SaleCode in ('0','U','UR')  group by AddressDir").show(false)
// +----------+------------------+--------+
// |AddressDir|avg(Price)        |count(1)|
// +----------+------------------+--------+
// |null      |197092.66077052505|46358   |
// |E         |123251.17108433734|415     |
// |N         |190212.7973856209 |459     |
// |W         |155784.19010416666|384     |
// |S         |228409.2875695733 |539     |
// +----------+------------------+--------+
// *** the properties face south have a higher average price

// sales code
sql("select count(distinct SaleDesc) from PropertySaleView").show(false)  // 28, show(false) to show all column content
sql("select distinct SaleDesc as SaleDescription from PropertySaleView").show(false)

sql("select count(distinct SaleCode) from PropertySaleView").show(false)  // 47

// Sale validation by type
sql("select case when SaleCode = 'AA' then 'Undetermined' when SaleCode in ('0','U','UR') then 'ValidSale' when SaleCode in ('1','32','2','GV','3','6','9','13','14','16','19','27','33','34','35','36','37','99','BK','DT','H','N','PA') then 'InvalideSale' else 'DiscontinuedCodes' end as Type, count(1) from PropertySaleView group by Type").show(false)

// valide sale's average price v.s. general average price
sql("select avg(Price) from PropertySaleView where SaleCode in ('0','U','UR')").show(false) // 196,411
sql("select avg(Price) from PropertySaleView").show(false) // 169,560

// top 20 the expensivist school district among valide sale
sql("select count(distinct SchoolDesc) from PropertySaleView").show(false) //46
// 
sql("select SchoolDesc, avg(Price),count(1) from PropertySaleView where SaleCode in ('0','U','UR') group by SchoolDesc order by avg(Price) desc").show(false)


// write to Hive
PropertySale.write.saveAsTable("propertysale.PropertySale")

