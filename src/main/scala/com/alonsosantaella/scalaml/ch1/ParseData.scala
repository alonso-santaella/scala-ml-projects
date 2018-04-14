package com.alonsosantaella.scalaml.ch1

object ParseData {

  import org.apache.spark.sql._

  // Defining parameters to be used down the road
  // Path where training and testing data are:
  //val dataPath: String = "hdfs://master:9000/test/data/ch1/"
  val dataPath: String = "data/ch1/"
  val train: String = dataPath + "train.csv"
  val test: String = dataPath + "test.csv"

  // Fraction of rows to be sampled by sample()
  val trainSample: Double = 1.0
  val testSample: Double = 1.0

  /*
  Create SparkSession from CreateSparkSession.createSession method
  The name of the session/spark application is "ParseData"
  */
  val spark = CreateSparkSession.createSession("ParseData")

  /*
  Importing spark implicits so data frame schema is inferred implicitly
  when reading the data CSVs.
  Apparently, spark.implicits is an object, method or whatever of the
  SparkSession (val = spark). For some reason IntelliJ says we ain't using it
  */
  //import spark.implicits._

  println(s"Reading data from $train file...")

  val trainData: DataFrame = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .format("csv")
    .load(train)
    .cache

  println(s"Reading data from $test file...")

  val testData: DataFrame = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .format("csv")
    .load(test)
    .cache

  // Schema of the training data frame:
  //println(trainData.printSchema())

  // Dimensions of the data frame:
  println(trainData.count(), trainData.columns.length)

  /*
  The "loss" column name is changed to "label", as the linear regression
  methods look for a column named as such. The data is then sampled
  without replacement, and is checked for NA rows.
  */

  println("Cleaning and parsing data:")
  println("Changing \"loss\" column to \"label\"...")
  //println(s"Sampling data with sample factor: $testSample...")
  println("Checking data for null rows...")

  val data: DataFrame =
    trainData
      .withColumnRenamed("loss", "label")
      .sample(false, trainSample)
      .removeNA()

  //data.show()

  implicit class ParseRemoveNA(df: DataFrame) {
    def removeNA(): DataFrame = {
      val DF: DataFrame = df.na.drop()

      // Checking for nulls or NAs:
      if (df == DF) {
        println("No null rows in the data.")
        df
      }
      else {
        println("The data contains null rows. Dropping them...")
        DF
      }
    }
  }
}
