from pyspark import SparkContext

sc = SparkContext()

logsPath="/Users/xiaoxiao/Documents/BigData/udemy_sample/cours/Spark/InstallGuideAndSourceCode/Datasets/LogsExample/hbase.log"

# create logs RDD
logs = sc.textFile(logsPath)

# filter all the line who has ERROR
logs.filter(lambda x: "ERROR" in x).take(10)

# create an accumulator variable, initial value: 0
errCount=sc.accumulator(0)


# function to parse the logs 
def processLog(line):
    
    # to insure use the variable defined outside function
    global errCount
    #dateField=''
    #logField=''
    # accumulator is incremented
    if "ERROR" in line:
        dateField=line[:24]
        logField=line[24:]
        errCount+=1
        return (dateField,logField)


print(errCount.value)

processedLogsPath="/Users/xiaoxiao/Documents/BigData/Python/spark/parsedLog_notnull.log"
# save parsed log to a file
logs.map(processLog).filter(lambda x: x is not None).saveAsTextFile(processedLogsPath)