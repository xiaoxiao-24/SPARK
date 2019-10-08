//first step in sclala

val data = Array(1,2,3,4,5)
val distData = sc.parallelize(data)

distData.reduce((a,b) => a + b)

//get file from repository
//calculate length of lines
val lines = sc.textFile("C:\\Users\\Xiaoxiao\\Documents\\SVN_IP_mfps.txt")
val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a,b)=>a+b)

//print each line
lines.foreach(println)