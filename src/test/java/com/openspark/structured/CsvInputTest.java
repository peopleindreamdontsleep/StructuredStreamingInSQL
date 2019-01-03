package com.openspark.structured;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class CsvInputTest {

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "1")
                .config("spark.sql.shuffle.partitions", "1")
                .appName("CsvInputTest")
                .master("local[2]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        StructType userSchema = new StructType().add("name", "string").add("age", "integer");
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ";")
                .schema(userSchema)
                // Specify schema of the csv files  是个文件目录
                .csv("F:\\E\\wordspace\\sqlstream\\filepath");
        // Returns True for DataFrames that have streaming sources
        System.out.println(csvDF.isStreaming());
        //csvDF.printSchema();
        //Dataset<Row> wordCounts = spark.sql("select name,sum(age) from table group by name");

        StreamingQuery query = csvDF.writeStream()
                .format("console")
                .outputMode("append")
                .trigger(ProcessingTime("2 seconds"))
                .start();

        query.awaitTermination();
    }
}
