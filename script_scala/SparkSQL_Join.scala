// Spark SQL join


// exemple 1 

// dataframe 1
val makerspace = spark.read.option("header","true").csv("/user/xiaoxiao/uk-makerspaces-identifiable-data.csv")

// dataframe 2
val postCode = spark.read.option("header","true").csv("/user/xiaoxiao/uk-postcode.csv").withColumn("PostCode1",concat(col("PostCode"), lit(" ")))

// join
val joined = makerspace.join(postCode, makerspace.col("Postcode").startsWith(postCode.col("PostCode1")),"left_outer")

joined.groupBy("Region").count().show(200)


// exemple 2

val left = Seq((0, "apple"), (1, "pair")).toDF("id", "fruit")
val right = Seq((0, "red"), (2, "yellow"), (3, "blue")).toDF("id", "color")
val right2 = Seq((0, "red"), (2, "yellow"), (3, "blue")).toDF("identifier", "color")

val joined = left.join(right, "id").show
val joined = left.join(right, Seq("id"),"leftouter").show
val joined = left.join(right, Seq("id"),"rightouter").show
val joined = left.join(right, Seq("id"),"fullouter").show

val joined = left.join(right2, $"id"===$"identifier").show
val joined = left.join(right2, left("id")===right2("identifier")).show


// exemple 3
// type-preserving joins

case class Person(id: Long, name: String, cityId: Long)
case class City(id: Long, name: String)

val family = Seq(
  Person(0, "Agata", 0),
  Person(1, "Iweta", 0),
  Person(2, "Patryk", 2),
  Person(3, "Maksym", 0)).toDS
val cities = Seq(
  City(0, "Warsaw"),
  City(1, "Washington"),
  City(2, "Sopot")).toDS

val joined = family.joinWith(cities, family("cityId") === cities("id"))
