package com.openspark.sqlstream.source;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.util.DtStringUtil;
import com.openspark.sqlstream.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvInput implements BaseInput{

    Map<String,Object> csvMap = null;
    StructType schema = null;
    //生成datastream
    @Override
    public Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config) {
        csvMap = config.getPropMap();
        checkConfig();
        beforeInput();

        List<StructField> fields = new ArrayList<>();
        //get field and type info
        String fieldsInfoStr = config.getFieldsInfoStr();
        //获取具有schema的dataset
        String[] fieldRows = DtStringUtil.splitIgnoreQuotaBrackets(fieldsInfoStr, ",");
        for(String fieldRow : fieldRows){
            fieldRow = fieldRow.trim();
            String[] filedInfoArr = fieldRow.split("\\s+");
            if(filedInfoArr.length < 2){
                throw new RuntimeException("the legth of "+fieldRow+" is not right");
            }
            //Compatible situation may arise in space in the fieldName
            String filedName = filedInfoArr[0].toUpperCase();
            String filedType = filedInfoArr[1].toLowerCase();
            StructField field = DataTypes.createStructField(filedName, DtStringUtil.strConverDataType(filedType), true);
            fields.add(field);
        }
        //DataType stringType = DataTypes.StringType;
        schema= DataTypes.createStructType(fields);
        Dataset<Row> lineRow = prepare(spark);
        return lineRow;
    }

    //给一个默认的分隔符
    @Override
    public void beforeInput() {
        String delimiter = null;
        try {
            delimiter = csvMap.get("delimiter").toString();
        }catch (Exception e){
            System.out.println("分隔符未配置，默认为逗号");
        }
        if(delimiter==null){
            csvMap.put("delimiter",",");
        }
    }

    @Override
    public void afterInput() {

    }

    //检查config
    @Override
    public void checkConfig() {
        Boolean isValid = csvMap.containsKey("path") &&
                !csvMap.get("path").toString().trim().isEmpty();
        if(!isValid){
            throw new RuntimeException("path are needed in csvinput input and cant be empty");
            //System.exit(-1);
        }
    }

    //将生成的datastream转化为具有field的形式
    @Override
    public Dataset<Row> prepare(SparkSession spark) {
        Dataset<Row> lines = spark
                .readStream()
                .option("sep", csvMap.get("delimiter").toString())
                .schema(schema)
                // Specify schema of the csv files  是个文件目录
                .csv(csvMap.get("path").toString());
        return lines;
    }

    public String getName(){
        return "name";
    }



}
