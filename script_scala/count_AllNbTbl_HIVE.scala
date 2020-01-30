/*
  this script count the number of tables in each database in HIVE
    1) list all databases
    2) count number of tables in each database
*/

val df_db = sql("show databases")
//val list_db = df_db.toList()
val list_db = df_db.select("databaseName").collect().map(_(0)).toList
var item = ""; 
          
// For loop with collection
for( item <- list_db){ 
    sql(s"""use $item""")
    var df_tbl = sql("show tables")
    var nb_tbl = df_tbl.count()
    println(item + ":" +nb_tbl)
} 

