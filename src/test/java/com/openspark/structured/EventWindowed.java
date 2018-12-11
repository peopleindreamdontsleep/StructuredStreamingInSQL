package com.openspark.structured;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.sql.Timestamp;

public class EventWindowed {

    public static void main(String[] args) throws Exception {
        String windowDuration = "5" + " seconds";
        String slideDuration = "5" + " seconds";

        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "1")
                .config("spark.sql.shuffle.partitions", "1")
                .appName("JavaStructuredNetworkWordCount")
                .master("local[2]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "hadoop-sh1-core1")
                .option("port", 9998)
                //.option("includeTimestamp", true)
                .load();
        spark.sparkContext().setLogLevel("WARN");
        // Split the lines into words, retaining timestamps
        Dataset<Row> words = lines
                .as(Encoders.STRING())
                .map((MapFunction<String, EventString>) t -> {
                            String[] split = t.split(",");
                            return new EventString(Timestamp.valueOf(split[0]), split[1]);
                        },
                        Encoders.bean(EventString.class)
                ).toDF("timestamp", "word");

        // Group the data by window and word and compute the count of each group
        //words.withWatermark("","")

        Dataset<Row> rowDataset = words.withColumn("window", functions.window(words.col("timestamp"), windowDuration, slideDuration));

        rowDataset.printSchema();
        Dataset<Row> windowedCounts = rowDataset
                .groupBy(
                        rowDataset.col("window"),
                        rowDataset.col("word")
                ).count();

        // Start running the query that prints the windowed word counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
