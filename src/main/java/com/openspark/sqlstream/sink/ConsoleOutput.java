package com.openspark.sqlstream.sink;

import com.openspark.sqlstream.parser.CreateTableParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Map;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class ConsoleOutput implements BaseOuput {

    Map<String, Object> propMap = null;

    @Override
    public void checkConfig() {
        String outputMode = null;
        try {
            outputMode = propMap.get("outputmode").toString();
        } catch (Exception e) {
            System.out.println("outputMode未配置，默认是 complete");
        }
        if (outputMode == null) {
            propMap.put("outputmode", "complete");
        }

    }

    @Override
    public StreamingQuery process(SparkSession spark, Dataset<Row> dataset, CreateTableParser.SqlParserResult config) {

        StreamingQuery query = null;

        propMap = config.getPropMap();
        checkConfig();
        //TODO 后面改进 情况可能更多trigger(Trigger.Continuous("1 second")) trigger(Trigger.Once())
        if (propMap.containsKey("process")) {
            String process = getProcessTime(propMap.get("process").toString());
            query = dataset.writeStream()
                    .outputMode(propMap.get("outputmode").toString())
                    .format("console")
                    .option("truncate", "false")
                    .trigger(ProcessingTime(process))
                    .start();
        } else {
            query = dataset.writeStream()
                    .outputMode(propMap.get("outputmode").toString())
                    .option("truncate", "false")
                    .format("console")
                    .start();
        }

        return query;
    }

    public static String getProcessTime(String proTimeStr) {
        //String processTime = "2 seconds";
        String number;
        String time;
        number = proTimeStr.replaceAll("[^(0-9)]", "");
        time = proTimeStr.replaceAll("[^(A-Za-z)]", "");
        switch (time) {
            case "s":
            case "S":
                return number + " seconds";
        }
        throw new RuntimeException("process的时间是非法的");
    }

    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        return null;
    }
}
