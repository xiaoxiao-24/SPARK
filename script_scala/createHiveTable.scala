// create hive table from Spark SQL

import org.apache.spark.sql.SparkSession
val warehouselocation = "spark-warehouse"
val spark = SparkSession.builder().appName("hive exe").config("spark.sql.warehouse.dir",warehouselocation).enableHiveSupport().getOrCreate()

import spark.sql
sql("create table if not exists src (key INT, value STRING)")