package com.openspark.sqlstream.util;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.parser.SqlTree;
import com.openspark.sqlstream.sink.BaseOuput;
import com.openspark.sqlstream.source.BaseInput;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

import static com.openspark.sqlstream.util.DtStringUtil.strConverType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class SparkUtil {

    static String sourceBasePackage = "com.openspark.sqlstream.source.";
    static String sinkBasePackage = "com.openspark.sqlstream.sink.";

    //将input的内容注册成带schame的dataset
    public static Dataset<Row>  createDataSet(Dataset<Row> lineRow, String fieldsInfoStr, String lineDelimit){

        List<StructField> fields = new ArrayList<>();

        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();

        String[] fieldRows = DtStringUtil.splitIgnoreQuotaBrackets(fieldsInfoStr, ",");
        MetadataBuilder b = new MetadataBuilder();
        for(String fieldRow : fieldRows){
            fieldRow = fieldRow.trim();
            String[] filedInfoArr = fieldRow.split("\\s+");
            if(filedInfoArr.length < 2){
                throw new RuntimeException("the legth of "+fieldRow+" is not right");
            }
            //Compatible situation may arise in space in the fieldName
            String filedName = filedInfoArr[0].toUpperCase();
            fieldNames.add(filedName);
            String filedType = filedInfoArr[1].toLowerCase();
            fieldTypes.add(filedType);
            StructField field = DataTypes.createStructField(filedName, DtStringUtil.strConverDataType(filedType), true,b.build());
            fields.add(field);
        }
        //DataType stringType = DataTypes.StringType;
        StructType schema = DataTypes.createStructType(fields);
        Dataset<String> words1 = lineRow
                .as(Encoders.STRING())
                .mapPartitions(new MapPartitionsFunction<String, String>() {
                    @Override
                    public Iterator<String> call(Iterator<String> input) throws Exception {
                        List<String> recordList = new ArrayList<>();
                        JSONObject jsonObject = new JSONObject();
                        while (input.hasNext()){
                            String record = input.next();
                            String[] split = record.split(lineDelimit);
                            for (int i = 0; i <split.length ; i++) {
                                try{
                                    jsonObject.put(fieldNames.get(i),strConverType((split[i]),fieldTypes.get(i)));
                                }catch (Exception e){
                                    e.printStackTrace();
                                    System.out.println("erro line:"+record);
                                }
                            }
                            recordList.add(jsonObject.toString());
                        }
                        return recordList.iterator();
                    }
                },Encoders.STRING());

        String[] seExpr = new String[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            seExpr[i] = "v."+fieldNames.get(i);
        }

        Dataset<Row> transDataSet = words1
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"),schema).as("v"))
                .selectExpr(seExpr);

        return transDataSet;
    }

    public static BaseInput getSourceByClass(String className){
        BaseInput inputBase =null;
        try {
            inputBase = Class.forName(sourceBasePackage + className).asSubclass(BaseInput.class).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return inputBase;
    }

    public static Map<String,Dataset<Row>> getTableList(SparkSession spark,SqlTree sqlTree){
        Map<String, Dataset<Row>> rowTableList = new HashMap<>();
        Map<String, CreateTableParser.SqlParserResult> preDealTableMap = sqlTree.getPreDealTableMap();
        for (String key:preDealTableMap.keySet()) {
            //key是每个table的名字
            String type = (String)sqlTree.getPreDealTableMap().get(key).getPropMap().get("type");
            String upperType  = DtStringUtil.upperCaseFirstChar(type)+"Input";
            //反射获取对象
            BaseInput sourceByClass = SparkUtil.getSourceByClass(upperType);
            //将对象的getstream方法获得具有schema的dataset
            rowTableList.put(key,sourceByClass.getDataSetStream(spark,preDealTableMap.get(key)));
        }
        return rowTableList;
    }

    public static BaseOuput getSinkByClass(String className){
        BaseOuput outputBase =null;
        try {
            outputBase = Class.forName(sinkBasePackage + className).asSubclass(BaseOuput.class).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return outputBase;
    }

    public static StreamingQuery tableOutput(SparkSession spark,String targetTable,Dataset<Row> queryResult,Map<String, CreateTableParser.SqlParserResult> preDealSinkMap){

        String type = preDealSinkMap.get(targetTable).getPropMap().get("type").toString();
        String outputName = DtStringUtil.upperCaseFirstChar(type.toLowerCase()) + "Output";
        BaseOuput sinkByClass = getSinkByClass(outputName);
        StreamingQuery process = sinkByClass.process(spark, queryResult,preDealSinkMap.get(targetTable));
        return process;
    }
}
