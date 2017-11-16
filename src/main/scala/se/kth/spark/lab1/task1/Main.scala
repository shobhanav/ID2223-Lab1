package se.kth.spark.lab1.task1

import se.kth.spark.lab1._

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class Song(year: Double, f1: Double, f2: Double, f3: Double)

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("lab1").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    

    import sqlContext.implicits._    
    import sqlContext._

    val filePath = "src/main/resources/millionsong.txt"
    //val rawDF = ??

    val rdd = sc.textFile(filePath)

    //Step1: print the first 5 rows, what is the delimiter, number of features and the data types?
    println(rdd.take(5).toList)

    //Step2: split each row into an array of features    
    val recordsRdd = rdd.map(x => x.split(","))

    //Step3: map each row into a Song object by using the year label and the first three features
    val songsRdd = recordsRdd.map(x => Song(x(0).toDouble,x(1).toDouble,x(2).toDouble, x(3).toDouble))
    

    //Step4: convert your rdd into a dataframe
    val songsDf = songsRdd.toDF()
    
    val size = songsDf.count()
    
    println(s"1. Total no. of songs : $size")
    
    songsDf.createOrReplaceTempView("songs")
    val cnt = sqlContext.sql("SELECT * FROM songs WHERE year BETWEEN 1998.0 AND 2000.12").count()
    println(s"2. No. of songs between 1998 and 2000: $cnt")
    
    val a = songsDf.agg(min("year")).collect()
    println(s"minimum year :$a")
    
    val b = songsDf.agg(max("year")).collect()
    println(s"maximum year :$b")
    
    val c = songsDf.agg(avg("year")).collect()
    println(s"average year :$c")
    
    
    
    
    
  }
}