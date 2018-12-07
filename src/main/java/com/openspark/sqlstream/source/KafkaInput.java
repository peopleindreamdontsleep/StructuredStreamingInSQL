package com.openspark.sqlstream.source;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class KafkaInput implements BaseInput{

    Map<String,Object> kafkaMap = null;

    //生成datastream
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
        return SparkUtil.createDataSet(lineRow,fieldsInfoStr,lineDelimit);
    }

    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = kafkaMap.get("delimiter").toString();
        }catch (Exception e){
            System.out.println("分隔符未配置，默认为逗号");
        }
        if(delimiter==null){
            kafkaMap.put("delimiter",",");
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
        if(!isValid){
            throw new RuntimeException("topics and kafka.bootstrap.servers and group.id are needed in kafka input and cant be empty");
            //System.exit(-1);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaMap.get("kafka.bootstrap.servers").toString())
                .option("subscribe", kafkaMap.get("subscribe").toString())
                .option("group.id",kafkaMap.get("group").toString())
                .load();
        Dataset<Row> rowDataset = lines.selectExpr("CAST(value AS STRING)");
        return rowDataset;
    }

    public String getName(){
        return "name";
    }

}
