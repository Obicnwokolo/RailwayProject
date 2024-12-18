package spark

import org.apache.spark.{SparkConf, SparkContext}

object TrainRides{
  def main(args: Array[String]): Unit = {
    // Step 1: Initialize Spark Session
    val spark = SparkSession.builder()
      .appName {
        "Spark Transformations Example"
      }
      .master("local[*]") // Run locally with all available cores
      .getOrCreate()

    // Step 2: Load Data from CSV
    val filePath = "C:/Users/chigb/Downloads/railway.csv"
    val data = spark.read
      .option("header", "true") // File has headers
      .option("inferSchema", "true") // Infer data types
      .csv(filePath)

    println("Original Data:")
    data.show()


    spark.stop()
  }
}
