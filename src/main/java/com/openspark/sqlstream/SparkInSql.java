package com.openspark.sqlstream;

import com.openspark.sqlstream.parser.CreateTableParser;
import com.openspark.sqlstream.parser.InsertSqlParser;
import com.openspark.sqlstream.parser.SqlParser;
import com.openspark.sqlstream.parser.SqlTree;
import com.openspark.sqlstream.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.List;
import java.util.Map;

import static com.openspark.sqlstream.util.DtStringUtil.readToString;

public class SparkInSql {

    public static void main(String[] args) throws Exception {
        //TODO 传入的参数进行解析
        SparkSession spark = SparkSession
                .builder()
                .config("spark.default.parallelism", "2")
                .config("spark.sql.shuffle.partitions", "2")
                .appName("SparkInSql")
                .master("local[2]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        String sqlPath = "F:\\E\\wordspace\\sqlstream\\conf\\socketprostream";
        String sql = readToString(sqlPath);

        SqlTree sqlTree = SqlParser.parseSql(sql);
        Map<String, Dataset<Row>> tableList = SparkUtil.getTableList(spark, sqlTree);

        //List<InsertSqlParser.SqlParseResult> execSqlList = sqlTree.getExecSqlList();

        InsertSqlParser.SqlParseResult sqlParseResult = sqlTree.getExecSql();
        //获取插入语句中的 目标表
        String targetTable = sqlParseResult.getTargetTable();
        //获取插入语句中的相关表(先create的表)
        List<String> sourceTableList = sqlParseResult.getSourceTableList();
        //表是insert sql中target的时候才要注册成表
        //表示sql中source部分的时候要另外处理
        tableList.forEach((tableName, dataRow) -> {
            if (sourceTableList.contains(tableName)) {
                dataRow.printSchema();
                dataRow.createOrReplaceTempView(tableName);
            }
        });

        Dataset<Row> queryResult = spark.sql(sqlParseResult.getQuerySql());
        Map<String, CreateTableParser.SqlParserResult> preDealSinkMap = sqlTree.getPreDealSinkMap();
        StreamingQuery streamingQuery = SparkUtil.tableOutput(spark, targetTable, queryResult, preDealSinkMap);
        //query.lastProgress()
        streamingQuery.awaitTermination();

    }
}
