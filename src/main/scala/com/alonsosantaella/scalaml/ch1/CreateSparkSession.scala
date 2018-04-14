package com.alonsosantaella.scalaml.ch1

import org.apache.spark.sql.SparkSession

object CreateSparkSession {
  def createSession(name: String, master: String = "local[*]"): SparkSession = {
    val spark = SparkSession
      .builder
      .appName(name)
      .master(master)
      .getOrCreate()
    spark
  }
}
