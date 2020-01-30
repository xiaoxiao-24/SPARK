// create a dataset: employeeDS


// ---------------------------
// exemple 1: JSON to DataSet
// ---------------------------
// 1. Creating a class ‘Employee’ to store name and age of an employee.
case class Employee(name:String, age:Long)

// 2. Assigning a Dataset ‘caseClassDS’ to store the record of Andrew.
val caseClassDS = Seq(Employee("Andrew", 55)).toDS()
caseClassDS.show

// 3. set PATH of json file
val path = "/user/xiaoxiao/employee.json"
// create Dataset from file and class 'employee'
val employeeDS = spark.read.json(path).as[Employee]
employeeDS.show


// ---------------------------
// exemple 2:
// ---------------------------
// 1. Creating a primitive Dataset to demonstrate mapping of DataFrames into Datasets.
val primitiveDS = Seq(1,2,3).toDS()

// 2. Assigning the above sequence into an array.
primitiveDS.map(_+1).collect


// ---------------------------
// exemple 3: CSV to Dataset
// ---------------------------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

// 1. create schema
val customSchema = StructType(Seq(
StructField("userId",StringType,true),
StructField("color",StringType,true),
StructField("count",IntegerType,true)
)
)

// 2. create a class to link data to class
case class AbTest(userId: String, color: String, count: Integer)

// 3. import CSV
val resultDS = spark.read.schema(customSchema).option("delimiter",",").csv("C:\\Users\\Xiaoxiao\\Documents\\Git\\SPARK\\script_scala\\Comparison_RDD_DF_DS\\testevent.csv").as[AbTest]

// 4. group 
val finalResultDS = resultDS.groupBy("color").sum("count")

// 5. output
val outputDS = finalResultDS.collect()


// ---------------------------
// exemple 4: JSON to Dataset
// ---------------------------

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.Encoders

// define class
case class Person(name: String, age: Long)

// read json file, convert a DataFrame to a Dataset by using a class
val path = "C:\\Users\\Xiaoxiao\\Documents\\people.json"
val peopleDS = spark.read.json(path).as[Person]

// output
peopleDS.show()