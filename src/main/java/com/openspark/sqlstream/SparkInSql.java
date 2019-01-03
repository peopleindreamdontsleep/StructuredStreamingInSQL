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
import org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.openspark.sqlstream.util.DtStringUtil.readToString;
import static com.openspark.sqlstream.util.DynamicChangeUtil.getDataNode;
import static com.openspark.sqlstream.util.DynamicChangeUtil.getZkclient;

public class SparkInSql {
    //public static SqlTree sqlTree;

    public static void main(String[] args) throws Exception {

        while (true){
            if(!SparkUtil.isAdd){
                //TODO 传入的参数进行解析
                SparkSession spark = SparkUtil.getSparkSession();
                spark.sparkContext().setLogLevel("WARN");

                DynamicChangeUtil.CuratorWatcher(getZkclient(),"sqlstream/sql");
                SparkUtil.parseProcessAndSink(spark, SqlParser.sqlTree);

                spark.streams().addListener(new StreamingQueryListener() {
                    @Override
                    public void onQueryStarted(QueryStartedEvent queryStarted) {
                        System.out.println("Query started: " + queryStarted.id());
                    }
                    @Override
                    public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                        System.out.println("Query terminated: " + queryTerminated.id());
                    }
                    @Override
                    public void onQueryProgress(QueryProgressEvent queryProgress) {

                        System.out.println("Query made progress: " + queryProgress.progress().id());
                        int length = spark.sessionState().streamingQueryManager().active().length;
                        System.out.println("active query length:"+length);

                    }
                });
                SparkUtil.streamingQuery.awaitTermination();
            }
        }
    }
}
