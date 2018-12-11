package com.openspark.sqlstream.util;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.parser.SqlTree;
import com.openspark.sqlstream.sink.BaseOuput;
import com.openspark.sqlstream.source.BaseInput;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;

import static com.openspark.sqlstream.util.DtStringUtil.strConverType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class SparkUtil {

    static String sourceBasePackage = "com.openspark.sqlstream.source.";
    static String sinkBasePackage = "com.openspark.sqlstream.sink.";

    //将input的内容注册成带schame的dataset
    public static Dataset<Row> createDataSet(Dataset<Row> lineRow, String fieldsInfoStr, String lineDelimit, Map<String, Object> proMap) {
        Boolean isprocess = (Boolean) proMap.get("isProcess");
        //System.out.println("isprocess:"+isprocess);
        List<StructField> fields = new ArrayList<>();

        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();

        String[] fieldRows = DtStringUtil.splitIgnoreQuotaBrackets(fieldsInfoStr, ",");
        MetadataBuilder b = new MetadataBuilder();
        for (String fieldRow : fieldRows) {
            fieldRow = fieldRow.trim();
            String[] filedInfoArr = fieldRow.split("\\s+");
            if (filedInfoArr.length < 2) {
                throw new RuntimeException("the legth of " + fieldRow + " is not right");
            }
            //Compatible situation may arise in space in the fieldName
            String filedName = filedInfoArr[0].toUpperCase();
            fieldNames.add(filedName);
            String filedType = filedInfoArr[1].toLowerCase();
            fieldTypes.add(filedType);
            StructField field = DataTypes.createStructField(filedName, DtStringUtil.strConverDataType(filedType), true, b.build());
            fields.add(field);
        }
        if (isprocess) {
            StructField field = DataTypes.createStructField("timestamp", DtStringUtil.strConverDataType("timestamp"), true, b.build());
            fields.add(field);
            fieldNames.add("timestamp");
            fieldTypes.add("timestamp");
        }
        //DataType stringType = DataTypes.StringType;
        StructType schema = DataTypes.createStructType(fields);
        // System.out.println("schema:"+schema.length());
        Dataset<String> schemaLine = null;
        if (isprocess) {
            schemaLine = lineRow
                    .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                    .mapPartitions(new MapPartitionsFunction<Tuple2<String, Timestamp>, String>() {
                        @Override
                        public Iterator<String> call(Iterator<Tuple2<String, Timestamp>> input) throws Exception {
                            List<String> recordList = new ArrayList<>();
                            JSONObject jsonObject = new JSONObject();
                            while (input.hasNext()) {
                                Tuple2<String, Timestamp> record = input.next();
                                String[] split = record._1.split(lineDelimit);
                                for (int i = 0; i < split.length; i++) {
                                    try {
                                        jsonObject.put(fieldNames.get(i), strConverType((split[i]), fieldTypes.get(i)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        System.out.println("erro line:" + record);
                                    }
                                }
                                //process时，手动加上timestamp，所以这里schema加上timestamp
                                jsonObject.put("timestamp", record._2.toString());
                                recordList.add(jsonObject.toString());
                            }
                            return recordList.iterator();
                        }
                    }, Encoders.STRING());
        } else {
            //本来想采用动态生成javabean的方式，不过没成功就用了这种方式
            schemaLine = lineRow
                    .as(Encoders.STRING())
                    .mapPartitions(new MapPartitionsFunction<String, String>() {
                        @Override
                        public Iterator<String> call(Iterator<String> input) throws Exception {
                            List<String> recordList = new ArrayList<>();
                            JSONObject jsonObject = new JSONObject();
                            while (input.hasNext()) {
                                String record = input.next();
                                String[] split = record.split(lineDelimit);
                                for (int i = 0; i < split.length; i++) {
                                    try {
                                        jsonObject.put(fieldNames.get(i), strConverType((split[i]), fieldTypes.get(i)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        System.out.println("erro line:" + record);
                                    }
                                }
                                recordList.add(jsonObject.toString());
                            }
                            return recordList.iterator();
                        }
                    }, Encoders.STRING());

        }
        String[] seExpr = new String[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            seExpr[i] = "v." + fieldNames.get(i);
        }

        Dataset<Row> transDataSet = schemaLine
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).as("v"))
                .selectExpr(seExpr);

//window解析相关
        String windowType = getWindowType(proMap);

        Dataset<Row> datasetWithWindow = getDatasetWithWindow(transDataSet, windowType, proMap);

        return datasetWithWindow;
    }

    public static BaseInput getSourceByClass(String className) {
        BaseInput inputBase = null;
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

    //添加sql中增加window函数
    public static Map<String, Dataset<Row>> getTableList(SparkSession spark, SqlTree sqlTree) {
        Map<String, Dataset<Row>> rowTableList = new HashMap<>();
        Map<String, CreateTableParser.SqlParserResult> preDealTableMap = sqlTree.getPreDealTableMap();
        for (String key : preDealTableMap.keySet()) {
            //key是每个table的名字
            String type = (String) sqlTree.getPreDealTableMap().get(key).getPropMap().get("type");
            String upperType = DtStringUtil.upperCaseFirstChar(type) + "Input";
            //反射获取对象
            BaseInput sourceByClass = SparkUtil.getSourceByClass(upperType);
            //将对象的getstream方法获得具有schema的dataset
            rowTableList.put(key, sourceByClass.getDataSetStream(spark, preDealTableMap.get(key)));
        }
        return rowTableList;
    }

    public static BaseOuput getSinkByClass(String className) {
        BaseOuput outputBase = null;
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

    public static StreamingQuery tableOutput(SparkSession spark, String targetTable, Dataset<Row> queryResult, Map<String, CreateTableParser.SqlParserResult> preDealSinkMap) {

        String type = preDealSinkMap.get(targetTable).getPropMap().get("type").toString();
        String outputName = DtStringUtil.upperCaseFirstChar(type.toLowerCase()) + "Output";
        BaseOuput sinkByClass = getSinkByClass(outputName);
        StreamingQuery process = sinkByClass.process(spark, queryResult, preDealSinkMap.get(targetTable));
        return process;
    }


    public static Dataset<Row> getDatasetWithWindow(Dataset<Row> transDataSet, String windowType, Map<String, Object> proMap) {

        Dataset<Row> windowData = null;
        String timeField = "timestamp";
        Dataset<Row> waterMarkData = null;
        Boolean isProcess = (Boolean) proMap.get("isProcess");
        if (proMap.containsKey("watermark")) {
            if (!proMap.containsKey("eventfield") && !isProcess) {
                throw new RuntimeException("配置了event的watermark需要配置一个eventfield来和它配合呦");
            }
            if (isProcess) {
                waterMarkData = transDataSet.withWatermark("timestamp", proMap.get("watermark").toString());
            } else {
                timeField = proMap.get("eventfield").toString();
                waterMarkData = transDataSet.withWatermark(timeField, proMap.get("watermark").toString());
            }
        } else {
            waterMarkData = transDataSet;
        }
        //waterMarkData.printSchema();
        if (windowType.length() > 1) {
            String[] split = windowType.split("\\|");
            String[] splitTime = split[1].split(",");
            String windowDuration = "5 seconds";
            String slideDuration = "5 seconds";
            if (splitTime.length == 1) {
                windowDuration = splitTime[0].trim();
                slideDuration = splitTime[0].trim();
            } else if (splitTime.length == 2) {
                windowDuration = splitTime[0].trim();
                slideDuration = splitTime[1].trim();
            } else {
                throw new RuntimeException("window的配置的长度好像有点问题呦");
            }
            switch (split[0]) {
                case "event":
                    windowData = waterMarkData.withColumn("eventwindow", functions.window(waterMarkData.col(timeField), windowDuration, slideDuration));
                    break;
                case "process":
                    windowData = waterMarkData.withColumn("processwindow", functions.window(waterMarkData.col("timestamp"), windowDuration, slideDuration));
                    break;
                default:
                    windowData = waterMarkData;
                    break;
            }
        } else {
            windowData = transDataSet;
        }

        return windowData;
    }

    public static String getWindowType(Map<String, Object> proMap) {
        String windowType = "";
        String proWindow = "";
        String eventWindow = "";
        try {
            //判断是process类型
            proWindow = proMap.get("processwindow").toString();
            //判断window类型
        } catch (Exception e) {

        }
        try {
            //判断是event类型
            eventWindow = proMap.get("eventwindow").toString();
            //判断window类型
        } catch (Exception e) {

        }
        if (proWindow.length() > 1) {
            windowType = "process|" + proWindow;
        }
        if (eventWindow.length() > 1) {
            windowType = "event|" + eventWindow;
        }
        return windowType;
    }
}
