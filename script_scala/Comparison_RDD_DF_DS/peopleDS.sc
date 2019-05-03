// create Dataset

// define class
case class Person(name: String, age: Long)

// read json file, convert a DataFrame to a Dataset by using a class
val path = "C:\\Users\\Xiaoxiao\\Documents\\people.json"
val peopleDS = spark.read.json(path).as[Person]

// output
peopleDS.show()