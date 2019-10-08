import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def SaveRecord(rdd):
	host = 'sparkmaster.example.com'
	table = 'cats'
	keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
	valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
	conf = {"hbase.zookeeper.quorum": host,
		   "hbase.mapred.outputtable": table,
		   "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
		   "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
		   "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
	datamap = rdd.map(lambda x: (str(json.loads(x)["id"]), [str(json.loads(x["id"])),"cfamily","cats_json",x]))
	datamap.saveAsNewAPIHadoopDataset(conf=conf, keyConverter=keyConv,valueConverter=valueConv)
	
	
if __name__ == "__main__":
		sc = SparkContext(appName="Exemple")
		ssc = StreamingContext(sc, 10)
		
		lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
		lines.foreachRDD(SaveRecord)
		
		#lines.pprint()
		ssc.start()
		ssc.awaitTermination()