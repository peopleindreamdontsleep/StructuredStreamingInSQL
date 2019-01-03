package com.openspark.structured;

import net.sf.json.JSONObject;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.openspark.sqlstream.util.DtStringUtil.strConverType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class EventTimeJsonTest {

    private static Logger logger = LoggerFactory.getLogger(EventTimeJsonTest.class);

    public static void main(String[] args) throws StreamingQueryException, ClassNotFoundException {

        System.setProperty("hadoop.home.dir", "F:\\data\\hadooponwindows-master\\hadooponwindows-master");
        String windowDuration = "10" + " seconds";
        String slideDuration = "5" + " seconds";
        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "1")
                .config("spark.sql.shuffle.partitions", "1")
                .appName("EventTimeJsonTest")
                .master("local[2]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "hadoop-sh1-core1")
                .option("port", 9998)
                .load();

        List<String> field = new ArrayList<>();
        field.add("timestamp Timestamp");
        field.add("word String");
        //Dataset<WordCount> classDataset = lines.as(ExpressionEncoder.javaBean(WordCount.class));

        MetadataBuilder b = new MetadataBuilder();
        StructField[] fields = {
                new StructField("timestamp", DataTypes.TimestampType, true, b.build()),
                new StructField("word", DataTypes.StringType, true, b.build()),
        };
        StructType type = new StructType(fields);

//        Dataset<Row> words = lines
//                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
//                .mapPartitions(new MapPartitionsFunction<Tuple2<String, Timestamp>, Row>() {
//                    @Override
//                    public Iterator<Row> call(Iterator<Tuple2<String, Timestamp>> input) throws Exception {
//
//                        return null;
//                    }
//                }, Encoders.kryo(Row.class));

        Dataset<String> words1 = lines
                .as(Encoders.STRING())
                .mapPartitions(new MapPartitionsFunction<String, String>() {
                    @Override
                    public Iterator<String> call(Iterator<String> input) throws Exception {
                        List<String> recordList = new ArrayList<>();
                        JSONObject jsonObject = new JSONObject();
                        while (input.hasNext()) {
                            String record = input.next();
                            String[] split = record.split(",");
                            for (int i = 0; i < split.length; i++) {
                                try {
                                    jsonObject.put(field.get(i).split(" ")[0], strConverType(split[i], field.get(i).split(" ")[1]));
                                } catch (Exception e) {
                                    System.out.println("erro line:" + record);
                                }
                            }
                            System.out.println(jsonObject.toString());
                            recordList.add(jsonObject.toString());
                        }
                        return recordList.iterator();
                    }
                }, Encoders.STRING());

        Dataset<Row> d2 = words1
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), type).as("v"))
                .selectExpr("v.timestamp", "v.word");


        d2.printSchema();
        Dataset<Row> windowedCounts = d2.groupBy(
                functions.window(d2.col("timestamp"), windowDuration, slideDuration),
                d2.col("word")
        ).count().orderBy("window");

        StreamingQuery query = windowedCounts.writeStream()
                .format("console")
                .outputMode("complete")
                //.options()
                .option("truncate", "false")
                .trigger(ProcessingTime("2 seconds"))
                .start();

        query.awaitTermination();
    }
}
