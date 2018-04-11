package com.alonsosantaella.scalaml.ch1

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object AnalyzingInsuranceClaims {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val dataPath: String = "data/ch1/"
    val training: String = dataPath + "train.csv"
    val test: String = dataPath + "test.csv"

    // Create SparkSession interface for SparkSQL
    val spark = SparkSession
      .builder
      .appName("AnalyzingInsuranceClaims")
      .master("local[*]")
      .getOrCreate()

    // Importing spark implicits so data frame schema is inferred implicitly
    // when reading the data CSVs.
    // Apparently, spark.implicits is an object, method or whatever of the
    // SparkSession "spark"
    import spark.implicits._

    val trainingData = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .format("csv")
      .load(training)
      .cache

    val testData = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .format("csv")
      .load(test)
      .cache

    println(trainingData.printSchema())

  }
}
