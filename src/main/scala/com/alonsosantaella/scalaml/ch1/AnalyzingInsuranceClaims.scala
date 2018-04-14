package com.alonsosantaella.scalaml.ch1

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j._

//import com.alonsosantaella.scalaml.ch1.ParsingData._

object AnalyzingInsuranceClaims {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    /*
    Create SparkSession interface for SparkSQL
    The CreateSparkSession objects contains the createSession
    method which takes "master" and "appName" as parameters.
    */

    val spark = CreateSparkSession.createSession("AnalyzingInsuranceClaims")

    /*
    We extract the parsed data, called "data" from the ParseData object;
    "data" contains the data in "train.csv".
    The ParseData object does the following:
    - Reads the data CSVs (train.csv, test.csv),
    - Transforms them into SparkSQL's DataFrames with inferred schema,
    - Checks for null rows and drops them,
    - Partitions the "train.csv" into "training" and "validation" sets.
     */

    val df: DataFrame = ParseData.data

    println(df.printSchema())
  }
}
