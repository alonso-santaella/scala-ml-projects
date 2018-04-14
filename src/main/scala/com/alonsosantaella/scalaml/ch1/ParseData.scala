package com.alonsosantaella.scalaml.ch1

object ParseData {

  import org.apache.spark.sql._

  // Defining parameters to be used down the road
  // Path where training and testing data are:
  val dataPath: String = "data/ch1/"
  //val dataPath: String = "hdfs://master:9000/test/data/ch1/"
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
  SparkSession (val = spark). For some reason IntelliJ says we ain't using
  it so I commented it out.
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
  //println(trainData.count(), trainData.columns.length)

  /*
  The "loss" column name is changed to "label", as the linear regression
  methods look for a column named as such. The data is then sampled
  without replacement, and is checked for NA rows with the user-defined
  -method "removeNA" defined in the implicit class "ParseMethods".
  */

  println("Cleaning and parsing data:")
  println("Changing \"loss\" column to \"label\"...")
  //println(s"Sampling data with sample factor: $testSample...")
  println("Checking data for null rows...")

  val data: DataFrame =
    trainData
      .withColumnRenamed("loss", "label")
      .sample(withReplacement = false, trainSample)
      .removeNA() // This is a user-defined-method; see: "ParseMethods"

  /*
  The training data in "data" is now partitioned at random (with "seed")
  into a training (75%) and a validation (25%) data set; again, see
  splitTrVa method in ParseMethod class.
   */

  val (trainingData, validationData) = data.splitTrVa()

  // Caching the resulting data frames:
  trainingData.cache()
  validationData.cache()

  // Loading testingData:
  val testingData = testData
    .sample(withReplacement = false, testSample)
    .cache()

  /*
  Defining function to know which columns are categorical and which
  are continuous. Second function renames categorical variables.
  */

  def isCateg(c: String): Boolean = c.startsWith("cat")
  def categNewCol(c: String): String = if (isCateg(c)) s"idx_${c}" else c


  implicit class ParseMethods(df: DataFrame) {
    /*
    This method removes NAs from the data set and then compares it
    to the original data set; if the data contains no NA rows it returns
    the original data set, if the data contains null rows it returns the
    data set with those rows removed.
     */
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
    /*
    This method return a tuple with the training and validation
    dataframes. The seed and ratio of data to be used as training
    are coded as default values but can be modified if need be.
    */
    def splitTrVa(seed: Long = 12345, train: Double = 0.75) = {
      val valid: Double = 1-train
      val splits = df.randomSplit(Array(train, valid, seed))
      val (training, validation) = (splits(0), splits(1))
      (training,validation)
    }
  }

}
