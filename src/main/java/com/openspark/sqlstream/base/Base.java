package com.openspark.sqlstream.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Base {



    void checkConfig();

    /**
     * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
     */
    Dataset<Row> prepare(SparkSession spark);
}
