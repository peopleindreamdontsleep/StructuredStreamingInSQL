package com.openspark.structured;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;

import java.sql.Timestamp;

public class EventWindowed {

    public static void main(String[] args) throws Exception {
        String windowDuration = "10" + " seconds";
        String slideDuration = "5" + " seconds";

        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "1")
                .config("spark.sql.shuffle.partitions", "1")
                .appName("JavaStructuredNetworkWordCount")
                .master("local[2]")
                .getOrCreate();

        //spark.sparkContext().setLogLevel("WARN");
        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "106.14.133.121")
                .option("port", 8888)
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

        System.out.println(words.isStreaming());
        Dataset<Row> windowedCounts = words
                .withWatermark("timestamp", "10 seconds")
                .groupBy(
                        functions.window(words.col("timestamp"), "10 seconds", "5 seconds"),
                        words.col("word"))
                .count();

        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent queryStarted) {
                System.out.println("Query started: " + queryStarted.id());
            }
            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                System.out.println("Query terminated: " + queryTerminated.id());
            }
            @Override
            public void onQueryProgress(QueryProgressEvent queryProgress) {
                System.out.println("Query made progress: " + queryProgress.progress().prettyJson());
            }
        });


        // Group the data by window and word and compute the count of each group
//        Dataset<Row> timestamp = words.withWatermark("timestamp", "10 seconds");
//
//        Dataset<Row> rowDataset = timestamp.withColumn("window", functions.window(words.col("timestamp"), windowDuration, slideDuration));
//
//        rowDataset.printSchema();
//        Dataset<Row> windowedCounts = rowDataset
//                .groupBy(
//                        rowDataset.col("window"),
//                        rowDataset.col("word")
//                ).count();
//
//        spark.streams().addListener(new StreamingQueryListener() {
//            @Override
//            public void onQueryStarted(QueryStartedEvent event) {
//                System.out.println("Query started: " + event.id());
//            }
//
//            @Override
//            public void onQueryProgress(QueryProgressEvent event) {
//                System.out.println("Query terminated: " + event.progress().json());
//            }
//
//            @Override
//            public void onQueryTerminated(QueryTerminatedEvent event) {
//                System.out.println("Query terminated: " + event.id());
//            }
//        });

        // Start running the query that prints the windowed word counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
