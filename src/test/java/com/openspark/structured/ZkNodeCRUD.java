package com.openspark.structured;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import java.io.UnsupportedEncodingException;

import static com.openspark.sqlstream.util.DtStringUtil.readToString;
import static com.openspark.sqlstream.util.DynamicChangeUtil.getDataNode;
import static com.openspark.sqlstream.util.DynamicChangeUtil.getZkclient;

public class ZkNodeCRUD {

    public static void main(String[] args) throws Exception {
        try {
//            String dataNode = getDataNode(getZkclient(), "/sqlstream/sql");
//            System.out.println(dataNode);
            CuratorFramework zkClient = CuratorFrameworkFactory.newClient("172.18.250.93:2181", 5000, 5000,
                    new ExponentialBackoffRetry(3, 1000));
            zkClient.start();
            zkClient.blockUntilConnected();
            //String sqlPath = "F:\\E\\wordspace\\sqlstream\\conf\\csvsqlstream";
            String sqlPath = "F:\\E\\wordspace\\sqlstream\\conf\\sqlstream";
            String sql = readToString(sqlPath);
            zkClient.setData().forPath("/sqlstream/sql", sql.getBytes("UTF-8"));
        }
         catch (Exception e) {
            e.printStackTrace();
        }

    }
}
