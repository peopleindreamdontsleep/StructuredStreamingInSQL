package com.openspark.sqlstream.source;

import com.openspark.sqlstream.base.Base;
import com.openspark.sqlstream.parser.CreateTableParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public interface BaseInput extends Base {

    /**
     * No matter what kind of Input it is, all you have to do is create a DStream to be used latter
     */
    Dataset<Row> getDataSetStream(SparkSession spark, CreateTableParser.SqlParserResult config);


    void beforeInput();

    /**
     * Things to do after output, such as update offset
     */
    void afterInput();

    String getName();


}
