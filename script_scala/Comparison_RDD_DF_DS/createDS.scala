// create a dataset: employeeDS

// 1. Creating a class ‘Employee’ to store name and age of an employee.
case class Employee(name:String, age:Long)

// 2. Assigning a Dataset ‘caseClassDS’ to store the record of Andrew.
val caseClassDS = Seq(Employee("Andrew", 55)).toDS()
// Displaying the Dataset ‘caseClassDS’.
caseClassDS.show


// 3. set PATH of json file
val path = "/user/xiaoxiao/employee.json"
// create Dataset from file and class 'employee'
val employeeDS = spark.read.json(path).as[Employee]
employeeDS.show


// 1. Creating a primitive Dataset to demonstrate mapping of DataFrames into Datasets.
val primitiveDS = Seq(1,2,3).toDS()

// 2. Assigning the above sequence into an array.
primitiveDS.map(_+1).collect