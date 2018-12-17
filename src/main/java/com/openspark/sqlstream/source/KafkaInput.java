package com.openspark.sqlstream.source;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class KafkaInput implements BaseInput {

    Map<String, Object> kafkaMap = null;
    Boolean isProcess = true;

    //生成datastream
    //TODO 可以不加Processwindow
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config) {
        kafkaMap = config.getPropMap();
        checkConfig();
        beforeInput();
        Dataset<Row> lineRow = prepare(spark);
        final String lineDelimit = kafkaMap.get("delimiter").toString();
        //get field and type info
        String fieldsInfoStr = config.getFieldsInfoStr();
        //获取具有schema的dataset
        return SparkUtil.createDataSet(lineRow, fieldsInfoStr, lineDelimit, kafkaMap);
    }

    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = kafkaMap.get("delimiter").toString();
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        if (delimiter == null) {
            kafkaMap.put("delimiter", ",");
        }
        if (kafkaMap.containsKey("processwindow")) {
            isProcess = true;
            kafkaMap.put("isProcess", true);
        }
        if (kafkaMap.containsKey("eventwindow")) {
            isProcess = false;
            kafkaMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput() {

    }

    //检查config  topics
    @Override
    public void checkConfig() {
        Boolean isValid = kafkaMap.containsKey("subscribe") &&
                !kafkaMap.get("subscribe").toString().trim().isEmpty() &&
                kafkaMap.containsKey("kafka.bootstrap.servers") &&
                !kafkaMap.get("kafka.bootstrap.servers").toString().trim().isEmpty() &&
                kafkaMap.containsKey("group") &&
                !kafkaMap.get("group").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("topics and kafka.bootstrap.servers and group.id are needed in kafka input and cant be empty");
            //System.exit(-1);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Map<String, String> options = new HashMap<>();
        options.put("kafka.bootstrap.servers", kafkaMap.get("kafka.bootstrap.servers").toString());
        options.put("subscribe", kafkaMap.get("subscribe").toString());
        options.put("group.id", kafkaMap.get("group").toString());

        Dataset<Row> rowDataset = null;

        if (isProcess) {
            Dataset<Row> lines = spark
                    .readStream()
                    .format("kafka")
                    .options(options)
                    .option("includeTimestamp", true)
                    .load();
            rowDataset = lines.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        } else {
            Dataset<Row> lines = spark
                    .readStream()
                    .format("kafka")
                    .options(options)
                    .load();
            rowDataset = lines.selectExpr("CAST(value AS STRING)");
        }

        return rowDataset;
    }

    public String getName() {
        return "name";
    }

}
