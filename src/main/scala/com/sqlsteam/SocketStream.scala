package com.sqlsteam

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SocketStream {

  case class wordcount(word: String, wordvalue: Int)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .config("spark.default.parallelism", "1")
      .config("spark.sql.shuffle.partitions", "1")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val socketStreamDf = spark.readStream
      .format("socket")
      .option("host", "hadoop-sh1-core1")
      .option("port", 9998)
      .load()

    //implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Customer]

//    val salesDs = socketStreamDf
//      .map(record =>{
//        println(record.get(0))
//        val recordValues = record.get(0).toString().split(",")
//        wordcount(recordValues(0), recordValues(1).toInt)
//      })
    val schemaString = "name age"
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val schemaDf = spark.createDataFrame(socketStreamDf.rdd,schema)


    schemaDf.printSchema()
    val wordCount = spark.sql("select word,sum(wordvalue) from table group by word")

    val query = wordCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
