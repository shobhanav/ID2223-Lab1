package se.kth.spark.lab1.test

import se.kth.spark.lab1._
import org.apache.spark._
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.ml.tuning.{CrossValidator,CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vector

case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("lab1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._


    val filePath = "hdfs:///Projects/lab1_av/Resources/million-song-all.txt"
    
    val rawDF = sc.textFile(filePath).toDF("sentence")

    //Step1: tokenize each row
    val regexTokenizer = new RegexTokenizer()
    .setInputCol("sentence")
    .setOutputCol("words")
    .setPattern(",")

    //Step2: transform with tokenizer and show 5 rows    
    val tokenized = regexTokenizer.transform(rawDF)

    //Step3: transform array of tokens to a vector of tokens (use our ArrayToVector)
    val arr2Vect = new Array2Vector()    
    arr2Vect.setInputCol("words").setOutputCol("featureVector")
    
    //Step4: extract the label(year) into a new column
    val lSlicer = new VectorSlicer().setInputCol("featureVector").setOutputCol("year")
    lSlicer.setIndices(Array(0))

    //Step5: convert type of the label from vector to double (use our Vector2Double)
    val v2d = new Vector2DoubleUDF({a:Vector => a(0).toDouble})
    v2d.setInputCol("year").setOutputCol("yearDouble")
    
    //Step6: shift all labels by the value of minimum label such that the value of the smallest becomes 0 (use our DoubleUDF)
    val min = 1922.0
    val lShifter = new DoubleUDF(d => (d - min))
    lShifter.setInputCol("yearDouble").setOutputCol("yearShifted")
    
    //Step7: extract just the 3 first features in a new vector column
    val fSlicer = new VectorSlicer().setInputCol("featureVector").setOutputCol("features_all")
    fSlicer.setIndices(Array(1,2,3,4,5,6,7,8,9,10,11,12))
    
    //step8: LinearRegression
    val myLR = new LinearRegression().setLabelCol("yearDouble").
               setFeaturesCol("features_all").
               setMaxIter(10).
               setRegParam(0.1).
               setElasticNetParam(0.1)
    val lrStage = 6           

    //Step9: put everything together in a pipeline
    val pipeline = new Pipeline().setStages(Array(regexTokenizer,arr2Vect,lSlicer,v2d,lShifter,fSlicer,myLR))

    //Step10: generate model by fitting the rawDf into the pipeline
    val pipelineModel = pipeline.fit(rawDF)
    
    //get model summary and print RMSE
    val task3ModelSummary = pipelineModel.stages(lrStage).asInstanceOf[LinearRegressionModel].summary
    println(s"RMSE : ${task3ModelSummary.rootMeanSquaredError}")

    //Step11: transform data with the model - do predictions
    val predictions = pipelineModel.transform(rawDF)
    
    //Step12: drop all columns from the dataframe other than label and features
    val result = predictions.drop("sentence","words","featureVector","yearShifted","year")

    //print first k 
    result.take(5).toList.foreach(println)


    //build the parameter grid by setting the values for maxIter and regParam
    val paramGrid = new ParamGridBuilder().addGrid(myLR.regParam, Array(0.2,0.3,0.4, 0.01, 0.05, 0.001)).
                        addGrid(myLR.maxIter, Array(20,30,40, 3, 4, 5)).build()
    val evaluator = new RegressionEvaluator().setLabelCol("yearDouble")
    //create the cross validator and set estimator, evaluator, paramGrid
    val cv = new CrossValidator().setEstimator(pipeline).
                  setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(rawDF)
    val bestModelSummary = cvModel.bestModel.asInstanceOf[PipelineModel].
    stages(lrStage).asInstanceOf[LinearRegressionModel].summary
    
    
    //print best model RMSE to compare to previous    
    println(s"RMSE after hyperparameter tuning : ${bestModelSummary.rootMeanSquaredError}")
    
    val predictions2 = cvModel.transform(rawDF)
    val result2 = predictions2.drop("sentence","words","featureVector","yearShifted","year")
    
    //print first k
    result2.take(5).toList.foreach(println)

  }
}