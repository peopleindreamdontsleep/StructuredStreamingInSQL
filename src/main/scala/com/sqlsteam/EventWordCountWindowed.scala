package com.sqlsteam

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EventWordCountWindowed {

  def main(args: Array[String]) {
    val windowDuration = "10 seconds"
    val slideDuration = "5 seconds"

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCountWindowed")
      .config("spark.default.parallelism", "1")
      .config("spark.sql.shuffle.partitions", "1")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", "hadoop-sh1-core1")
      .option("port", 9998)
      //.option("includeTimestamp", true)
      .load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String)].map(line => {
      val split = line.split(",")
      (split(0), Timestamp.valueOf(split(1)))
    }).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
