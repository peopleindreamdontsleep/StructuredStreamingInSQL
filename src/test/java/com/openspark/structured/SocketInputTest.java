package com.openspark.structured;

import net.sf.json.JSONObject;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class SocketInputTest {

    private static Logger logger = LoggerFactory.getLogger(SocketInputTest.class);
    public static void main(String[] args) throws StreamingQueryException, ClassNotFoundException {

        System.setProperty("hadoop.home.dir","F:\\data\\hadooponwindows-master\\hadooponwindows-master" );

        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "1")
                .config("spark.sql.shuffle.partitions","1")
                .appName("JavaStructuredNetworkWordCount")
                .master("local[2]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");



        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("sep", " ")
                .option("host", "hadoop-sh1-core1")
                .option("port", 9998)
                .load();

        List<String> field = new ArrayList<>();
        field.add("name String");
        field.add("age Integer");
        //Dataset<WordCount> classDataset = lines.as(ExpressionEncoder.javaBean(WordCount.class));

        MetadataBuilder b = new MetadataBuilder();
        StructField[] fields={
                new StructField("name",DataTypes.IntegerType, true,b.build()),
                new StructField("age",DataTypes.IntegerType, true,b.build()),
        };
        StructType type = new StructType(fields);

        Dataset<String> words1 = lines
                .as(Encoders.STRING())
                .mapPartitions(new MapPartitionsFunction<String, String>() {
                    @Override
                    public Iterator<String> call(Iterator<String> input) throws Exception {
                        List<String> recordList = new ArrayList<>();
                        JSONObject jsonObject = new JSONObject();
                        while (input.hasNext()){
                            String record = input.next();
                            String[] split = record.split(",");
                            for (int i = 0; i <split.length ; i++) {
                                try{
                                    jsonObject.put(field.get(i).split(" ")[0],Integer.valueOf(split[i]));
                                }catch (Exception e){
                                    System.out.println("erro line:"+record);
                                }
                            }
                            recordList.add(jsonObject.toString());
                        }
                        return recordList.iterator();
                    }
                },Encoders.STRING());

        Dataset<Row> d2 = words1
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"),type).as("v"))
                .selectExpr("v.name","v.age");

        //Dataset<Row> dataFrame = spark.createDataFrame(words1.javaRDD(), type);
        d2.printSchema();
        d2.createOrReplaceTempView("table");
        Dataset<Row> wordCounts = spark.sql("select name,sum(age) from table group by name");

        StreamingQuery query = wordCounts.writeStream()
                .format("console")
                .outputMode("complete")
                .trigger(ProcessingTime("2 seconds"))
                .start();

        query.awaitTermination();
    }
}
