// txt file to DataFrame

// ?
import spark.implicits._

// ?
import org.apache.spark.sql.Encoder
import org.apache.spark.sal.catalyst.encoders.ExpressionEncoder // for RDD, who use encoders for serialization


// 1. Creating a class ‘Employee’ to store name and age of an employee.
case class Employee(name:String, age:Long)

// 2. create DF by reading from text file
val path_text = "/user/xiaoxiao/employee.txt"
val employeeDF = spark.sparkContext.textFile(path_text).map(_.split(",")).map(lines => Employee(lines(0),lines(1).trim.toInt)).toDF()

// 3. create tmp view
employeeDF.createOrReplaceTempView("employeeDFview")

// 4. define a DF with SparkSQL
val youngstersDF = spark.sql("select * from employeeDFview where age between 18 and 30")

youngstersDF.map(youngster => "name:" + youngster(0)).show
youngstersDF.map(youngster => "age:" + youngster(1)).show
youngstersDF.map(youngster => "name:" + youngster(0) + ", age:" + youngster(1)).show