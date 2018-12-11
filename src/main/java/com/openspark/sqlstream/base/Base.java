package com.openspark.sqlstream.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Base {

    void checkConfig();


    Dataset<Row> prepare(SparkSession spark);
}
