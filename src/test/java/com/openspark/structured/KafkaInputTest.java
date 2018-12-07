package com.openspark.structured;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class KafkaInputTest {

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "1")
                .config("spark.sql.shuffle.partitions","1")
                .appName("KafkaInputTest")
                .master("local[2]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        StructType userSchema = new StructType().add("word", "string").add("wordcount", "integer");
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "dfttshowkafka001:9092")
                .option("subscribe", "test")
                .option("group.id","test")
                .load();
        lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        lines.createOrReplaceTempView("table");
        Dataset<Row> wordCounts = spark.sql("select count(*) from table group by value");

        StreamingQuery query = wordCounts.writeStream()
                .format("console")
                .outputMode("complete")
                .trigger(ProcessingTime("2 seconds"))
                .start();

        query.awaitTermination();


    }
}
