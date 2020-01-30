// UDF string

val dataset = Seq((0,"heLLo","begin word"),(1,"worLd","end word")).toDF("id","text","cmt")

dataset.registerTempTable("dataset")

// import library for UDF
import org.apache.spark.sql.functions.udf


// Upper Case
// -------------------------------
// methode 1
// create a UDF: upper case <step 1>
val upper: String => String = _.toUpperCase
// create a UDF <step 2>
val upperUDF = udf(upper)
// use the UDF add a new column which upper the 'text' column
dataset.withColumn("upper",upperUDF('text)).show

// methode 2
spark.udf.register("myUpper",(input: String) => input.toUpperCase)

// methode 3
val pay = Seq((0,"John","man",3000,4),(1,"Lucie","woman",2500,5)).toDF("id","name","sex","salary","nb_month")
val toUpper  = udf[String,String](_.toUpperCase)
pay.select(toUpper(pay("sex"))).show
pay.withColumn("sex1",toUpper('sex)).show

// methode 4
def upper(s: String): String = s.toUpperCase
spark.udf.register("Upper", upper _)
spark.sql("select id, sex, Upper(sex) from Pay").show()


// list all the functions like %pper%
spark.catalog.listFunctions.filter('name like "%pper%").show(false)


// Lower Case
// -------------------------------
// create a UDF: lower case
val lower: String => String = _.toLowerCase
val UDF2Lower = udf(lower)
dataset.withColumn("lower",UDF2Lower('text)).show


// Substring
// -------------------------------
// create a UDF: substring
val substr: String => String = _.substring(1,3)
val UDF3substr = udf(substr)
dataset.withColumn("substr",UDF3substr('text)).show


// Split
// -------------------------------
// create a UDF: split
val split: String => Array[String] = _.split(" ")
val UDF4Split = udf(split)
dataset.withColumn("split",UDF4Split('cmt)).show


// Replace
// -------------------------------
// create a UDF: replace
val replace: String => String = _.replace("L","N")
val UDF5Replace = udf(replace)
dataset.withColumn("replace",UDF5Replace('text)).show


// Length
// -------------------------------
// create a UDF: length
val length: String => Int = _.length
val UDF6Length = udf(length)
dataset.withColumn("length",UDF6Length('text)).show



// create a UDF: concat (?)
val concat: Array[String] => String = concat(_(0),_(1))

val toConcat = udf[Array[String],String](_(0).concat(_(1)))

def conCat(a:String,b:String):String = {
    var res:String = ""
    res = a.concat(b)
    return res
}


// calculate
// -------------------------------
def addInt( a:Int, b:Int ) : Int = {
    var sum:Int = 0
    sum = a + b
    return sum
}

def multiInt(a: Int, b: Int):Int = {
    var mul:Int = 1
    mul = a*b
    return mul
}
