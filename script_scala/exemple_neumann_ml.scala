// --------------------------------------------------------------------
// ML
// --------------------------------------------------------------------
import sys.process._  
import scala.language.postfixOps

"ls" ! // this is how you run the command ls while within the spark-shell


import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator  
import org.apache.spark.mllib.evaluation.MulticlassMetrics  
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics  
import org.apache.spark.ml.classification.RandomForestClassifier  
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, CrossValidator}  
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoderEstimator}  
import org.apache.spark.ml.linalg.Vectors  
import org.apache.spark.ml.Pipeline  
import org.apache.log4j._  
Logger.getLogger("org").setLevel(Level.ERROR) 

// make dataset for train
import spark.sql

// one-hot encoding on our categorical features
// First we have to use the StringIndexer to convert the strings to integers.
implicit def bool2int(b:Boolean) = if (b) 1 else 0
spark.udf.register("bool2int", bool2int _)
val DF_Churn = sql("""select 
arpu,averagemonthlybill,
totalspent,
bool2int(smartphone),handsettype,
category,type,
daysactive,dayslastactive,
bool2int(canceled)
from neumanndb.imsi where canceled is not null and category <> '' and type <> ''
and handsettype <> ''
""")

import org.apache.spark.ml.feature.StringIndexer
val indexer1 = new StringIndexer().
    setInputCol("handsettype").
    setOutputCol("handsettypeIndex").
    setHandleInvalid("keep") 
val DF_Churn1 = indexer1.fit(DF_Churn).transform(DF_Churn)

val indexer2 = new StringIndexer().
    setInputCol("category").
    setOutputCol("categoryIndex").
    setHandleInvalid("keep")
val DF_Churn2 = indexer2.fit(DF_Churn1).transform(DF_Churn1)

val indexer3 = new StringIndexer().
    setInputCol("type").
    setOutputCol("typeIndex").
    setHandleInvalid("keep")
val DF_Churn3 = indexer3.fit(DF_Churn2).transform(DF_Churn2)


// Then we have to use the OneHotEncoderEstimator to do the encoding.
import org.apache.spark.ml.feature.OneHotEncoderEstimator
val encoder = new OneHotEncoderEstimator().
  setInputCols(Array("handsettypeIndex", "categoryIndex","typeIndex")).
  setOutputCols(Array("handsettypeVec", "categoryVec","typeVec"))
val DF_Churn_encoded = encoder.fit(DF_Churn3).transform(DF_Churn3)


// Spark models need exactly two columns: “label” and “features”
// Set the input columns as the features we want to use
val get_label = DF_Churn_encoded.select(DF_Churn_encoded("UDF:bool2int(canceled)").as("label"), 
          $"arpu", $"averagemonthlybill", $"totalspent", $"UDF:bool2int(smartphone)", $"daysactive", $"dayslastactive", $"handsettypeVec", $"categoryVec", $"typeVec")

import org.apache.spark.ml.feature.VectorAssembler
val assembler = new VectorAssembler().setInputCols(Array(
  "arpu", "averagemonthlybill", "totalspent", "UDF:bool2int(smartphone)", "daysactive", 
  "dayslastactive", "handsettypeVec", "categoryVec", "typeVec")).
   setOutputCol("features")


// Transform the DataFrame
val output = assembler.transform(get_label).select($"label",$"features")


// Splitting the data by create an array of the training and test data
val Array(training, test) = output.select("label","features").
                            randomSplit(Array(0.7, 0.3), seed = 12345)

// create the model
val rf = new RandomForestClassifier()

// create the param grid
val paramGrid = new ParamGridBuilder().
  addGrid(rf.numTrees,Array(20,50,100)).
  build()

// create cross val object, define scoring metric
val cv = new CrossValidator().
  setEstimator(rf).
  setEvaluator(new MulticlassClassificationEvaluator().setMetricName("weightedRecall")).
  setEstimatorParamMaps(paramGrid).
  setNumFolds(3).
  setParallelism(2)

// You can then treat this object as the model and use fit on it.
val model = cv.fit(training)


val results = model.transform(test).select("features", "label", "prediction")

val predictionAndLabels = results.
    select($"prediction",$"label").
    as[(Double, Double)].
    rdd

results.
    select("prediction","label").where("prediction<>label").count()
    as[(Double, Double)].
    rdd.collect()


// create our metrics objects and print out the confusion matrix.
// Instantiate a new metrics objects
val bMetrics = new BinaryClassificationMetrics(predictionAndLabels)
val mMetrics = new MulticlassMetrics(predictionAndLabels)
val labels = mMetrics.labels

// Print out the Confusion matrix
println("Confusion matrix:")
println(mMetrics.confusionMatrix)


// ---------------------------
// print metrics
// ---------------------------

// Precision by label
labels.foreach { l =>
  println(s"Precision($l) = " + mMetrics.precision(l))
}

// Recall by label
labels.foreach { l =>
  println(s"Recall($l) = " + mMetrics.recall(l))
}

// False positive rate by label
labels.foreach { l =>
  println(s"FPR($l) = " + mMetrics.falsePositiveRate(l))
}

// F-measure by label
labels.foreach { l =>
  println(s"F1-Score($l) = " + mMetrics.fMeasure(l))
}


// ------------------------------------------------------
// sophisticated metrics such as AUC and AUPRC
// ------------------------------------------------------
// Precision by threshold
val precision = bMetrics.precisionByThreshold
precision.foreach { case (t, p) =>
  println(s"Threshold: $t, Precision: $p")
}

// Recall by threshold
val recall = bMetrics.recallByThreshold
recall.foreach { case (t, r) =>
  println(s"Threshold: $t, Recall: $r")
}

// Precision-Recall Curve
val PRC = bMetrics.pr

// F-measure
val f1Score = bMetrics.fMeasureByThreshold
f1Score.foreach { case (t, f) =>
  println(s"Threshold: $t, F-score: $f, Beta = 1")
}

val beta = 0.5
val fScore = bMetrics.fMeasureByThreshold(beta)
fScore.foreach { case (t, f) =>
  println(s"Threshold: $t, F-score: $f, Beta = 0.5")
}

// AUPRC
val auPRC = bMetrics.areaUnderPR
println("Area under precision-recall curve = " + auPRC)

// Compute thresholds used in ROC and PR curves
val thresholds = precision.map(_._1)

// ROC Curve
val roc = bMetrics.roc

// AUROC
val auROC = bMetrics.areaUnderROC
println("Area under ROC = " + auROC)






