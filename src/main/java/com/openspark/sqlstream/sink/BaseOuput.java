package com.openspark.sqlstream.sink;

import com.openspark.sqlstream.base.Base;
import com.openspark.sqlstream.parser.CreateTableParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Map;

public interface BaseOuput extends Base {

    StreamingQuery process(SparkSession spark, Dataset<Row> dataset,CreateTableParser.SqlParserResult config);
}
