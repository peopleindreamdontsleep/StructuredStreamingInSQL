package com.openspark.sqlstream.source;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.util.DtStringUtil;
import com.openspark.sqlstream.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SocketInput implements BaseInput{

    Map<String,Object> socketMap = null;

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
        return SparkUtil.createDataSet(lineRow,fieldsInfoStr,lineDelimit);
    }

    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = socketMap.get("delimiter").toString();
        }catch (Exception e){
            System.out.println("分隔符未配置，默认为逗号");
        }
        if(delimiter==null){
            socketMap.put("delimiter",",");
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
        if(!isValid){
            throw new RuntimeException("host and port are needed in socket input and cant be empty");
            //System.exit(-1);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", socketMap.get("host").toString())
                .option("port", socketMap.get("port").toString())
                .load();
        return lines;
    }

    public String getName(){
        return "name";
    }



}
