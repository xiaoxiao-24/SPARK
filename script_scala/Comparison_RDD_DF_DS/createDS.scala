// create a dataset: employeeDS

// 1. Creating a class ‘Employee’ to store name and age of an employee.
case class Employee(name:String, age:Long)

// 2. Assigning a Dataset ‘caseClassDS’ to store the record of Andrew.
val caseClassDS = Seq(Employee("Andrew", 55)).toDS()

// 3. Displaying the Dataset ‘caseClassDS’.
caseClassDS.show


// 4. Creating a primitive Dataset to demonstrate mapping of DataFrames into Datasets.
val primitiveDS = Seq(1,2,3).toDS()

// 5. Assigning the above sequence into an array.
primitiveDS.map(_+1).collect