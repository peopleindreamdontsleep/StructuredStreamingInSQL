package com.openspark.sqlstream.source;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class SocketInput implements BaseInput {

    Map<String, Object> socketMap = null;
    String windowType = "";
    Boolean isProcess = true;

    //生成datastream
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config) {
        socketMap = config.getPropMap();
        checkConfig();
        beforeInput();
        Dataset<Row> lineRow = prepare(spark);
        final String lineDelimit = socketMap.get("delimiter").toString();
        //get field and type info
        String fieldsInfoStr = config.getFieldsInfoStr();
        //获取具有schema的dataset
        return SparkUtil.createDataSet(lineRow, fieldsInfoStr, lineDelimit, socketMap);
    }

    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        String proWindow = null;
        String eventWindow = null;
        try {
            delimiter = socketMap.get("delimiter").toString();
            //判断window类型
        } catch (Exception e) {
            System.out.println("分隔符未配置，默认为逗号");
        }
        try {
            proWindow = socketMap.get("processwindow").toString();
            eventWindow = socketMap.get("eventwindow").toString();
            //判断window类型
        } catch (Exception e) {

        }
        if (delimiter == null) {
            socketMap.put("delimiter", ",");
        }
        if (proWindow != null) {
            windowType = "process " + proWindow;
        }
        if (eventWindow != null) {
            windowType = "event " + eventWindow;
        }
        if (socketMap.containsKey("processwindow")) {
            isProcess = true;
            socketMap.put("isProcess", true);
        }
        if (socketMap.containsKey("eventwindow")) {
            isProcess = false;
            socketMap.put("isProcess", false);
        }
    }

    @Override
    public void afterInput() {

    }

    //检查config
    @Override
    public void checkConfig() {
        Boolean isValid = socketMap.containsKey("host") &&
                !socketMap.get("host").toString().trim().isEmpty() &&
                socketMap.containsKey("port") &&
                !socketMap.get("host").toString().trim().isEmpty();
        if (!isValid) {
            throw new RuntimeException("host and port are needed in socket input and cant be empty");
            //System.exit(-1);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Map<String, String> options = new HashMap<>();
        options.put("host", socketMap.get("host").toString());
        options.put("port", socketMap.get("port").toString());
        Dataset<Row> lines = null;
        if (isProcess) {
            //option("includeTimestamp", true)
            // options.put("includeTimestamp", true);
            lines = spark
                    .readStream()
                    .format("socket")
                    .options(options)
                    .option("includeTimestamp", true)
                    .load();
        } else {
            lines = spark
                    .readStream()
                    .format("socket")
                    .options(options)
                    .load();
        }
        return lines;
    }

    public String getName() {
        return "name";
    }


}
