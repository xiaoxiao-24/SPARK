
val path = "/Users/xiaoxiaorey/Downloads/AlleghenyPropertySale.csv"

// dataframe
val PropertySale = spark.read.option("header","true").option("delimiter",",").csv(path)
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

