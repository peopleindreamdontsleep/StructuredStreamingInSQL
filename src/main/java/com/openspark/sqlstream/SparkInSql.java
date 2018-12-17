package com.openspark.sqlstream;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.parser.InsertSqlParser;
import com.openspark.sqlstream.parser.SqlParser;
import com.openspark.sqlstream.parser.SqlTree;
import com.openspark.sqlstream.util.DynamicChangeUtil;
import com.openspark.sqlstream.util.SparkUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.List;
import java.util.Map;

import static com.openspark.sqlstream.util.DtStringUtil.readToString;
import static com.openspark.sqlstream.util.DynamicChangeUtil.getDataNode;
import static com.openspark.sqlstream.util.DynamicChangeUtil.getZkclient;

public class SparkInSql {
    public static SqlTree sqlTree;
    public static void main(String[] args) throws Exception {
        //TODO 传入的参数进行解析
        SparkSession spark = SparkUtil.getSparkSession();
        spark.sparkContext().setLogLevel("WARN");
        DynamicChangeUtil.CuratorWatcher(getZkclient(),"sqlstream/sql");
        SparkUtil.parseProcessAndSink(spark, SqlParser.sqlTree);

        SparkUtil.streamingQuery.awaitTermination();
        //String sqlPath = "F:\\E\\wordspace\\sqlstream\\conf\\csvsqlstream";
        //String sql = readToString(sqlPath);
        //String sql = getDataNode(getZkclient(),"/sqlstream/sql");
        //SqlParser.sqlTree = sql;

        //sql = DynamicChangeUtil.sqlStr;
        //SqlTree sqlTree = SqlParser.parseSql(sql);

//        Map<String, Dataset<Row>> tableList = SparkUtil.getTableList(spark, SqlParser.sqlTree);
//
//        //List<InsertSqlParser.SqlParseResult> execSqlList = sqlTree.getExecSqlList();
//
//        InsertSqlParser.SqlParseResult sqlParseResult = SqlParser.sqlTree.getExecSql();
//        //获取插入语句中的 目标表
//        String targetTable = sqlParseResult.getTargetTable();
//        //获取插入语句中的相关表(先create的表)
//        List<String> sourceTableList = sqlParseResult.getSourceTableList();
//        //表是insert sql中target的时候才要注册成表
//        //表示sql中source部分的时候要另外处理
//        tableList.forEach((tableName, dataRow) -> {
//            if (sourceTableList.contains(tableName)) {
//                dataRow.printSchema();
//                dataRow.createOrReplaceTempView(tableName);
//            }
//        });
//
//        Dataset<Row> queryResult = spark.sql(sqlParseResult.getQuerySql());
//
//        Map<String, CreateTableParser.SqlParserResult> preDealSinkMap = SqlParser.sqlTree.getPreDealSinkMap();



    }
}
