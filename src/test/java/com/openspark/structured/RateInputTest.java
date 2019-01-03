package com.openspark.structured;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryManager;

import java.util.UUID;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class RateInputTest {

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "10")
                .config("spark.sql.shuffle.partitions", "10")
                .appName("RateInputTest")
                .master("local[2]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        //StructType userSchema = new StructType().add("word", "string").add("wordcount", "integer");
        Dataset<Row> lines = spark
                .readStream()
                .format("rate")
                .load();
        lines.selectExpr("CAST(value AS STRING)");

        lines.createOrReplaceTempView("table");
        Dataset<Row> wordCounts = spark.sql("select value,count(*) from table group by value");

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
                UUID id = queryProgress.progress().id();

            }
        });
        StreamingQuery query = wordCounts.writeStream()
                .format("console")
                .outputMode("complete")
                .trigger(ProcessingTime("2 seconds"))
                .start();

        query.awaitTermination();
    }
}
