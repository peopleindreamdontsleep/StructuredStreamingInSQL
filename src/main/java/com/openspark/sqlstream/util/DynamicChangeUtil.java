package com.openspark.sqlstream.util;

import com.openspark.sqlstream.parser.SqlParser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class DynamicChangeUtil implements Watcher {

    public static String sqlStr = "";

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
            countDownLatch.countDown();
        }
    }

    public static void CuratorWatcher(CuratorFramework client, String sqlPath) {
        TreeCache treeCache = new TreeCache(client, "/" + sqlPath);
        //设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                String zkData = "";
                ChildData data = event.getData();
                TreeCacheEvent.Type eventType = event.getType();
                if (data != null) {
                    if (eventType == TreeCacheEvent.Type.NODE_ADDED) {
                        System.out.println("NODE_ADDED");
                        if(!zkData.trim().equals(""))
                            zkData = new String(data.getData());
                            //SparkUtil.refresh(zkData);
                    }
                    else if (eventType == TreeCacheEvent.Type.NODE_UPDATED) {
                        System.out.println("SQL更新了呦");
                        zkData = new String(data.getData());
                        //System.out.println(zkData);
                        SparkUtil.refresh(zkData);
                    }
                    else if (eventType == TreeCacheEvent.Type.NODE_REMOVED) {
                        System.out.println("NODE_REMOVED");
                        zkData = new String(data.getData());
                    }


                } else {
                    System.out.println("can get " + event.getType());
                }
            }
        });
        //开始监听
        try {
            treeCache.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getDataNode(CuratorFramework client, String path) throws Exception {
        byte[] datas = client.getData().forPath(path);
        return new String(datas);
    }

    public static void getValue(String value){
        sqlStr = value;
    }

    public static CuratorFramework getZkclient() throws InterruptedException {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient("172.18.250.93:2181", 5000, 5000,
                new ExponentialBackoffRetry(3, 1000));
        zkClient.start();
        zkClient.blockUntilConnected();
        return zkClient;
    }

    public static CuratorFramework getClient(String group) {
        int connectionTimeoutMs = 5000;
        String namespace = group;
        CuratorFramework client = CuratorFrameworkFactory.builder().
                connectionTimeoutMs(connectionTimeoutMs).
                connectString(ConfigConstrant.ZOOKEPER_ADRESS).
                namespace(namespace).
                retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).build();
        client.start();
        return client;
    }

    public static void main(String[] args) throws Exception{
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient("172.18.250.93:2181",
                new ExponentialBackoffRetry(3, 1000));
        zkClient.start();
        zkClient.blockUntilConnected();
        String sqlData = getDataNode(zkClient,"/sqlstream/sql");
        System.out.println("sqlData "+sqlData);
        DynamicChangeUtil.CuratorWatcher(zkClient,"sqlstream/sql");
        sqlData = sqlStr;
        System.out.println("sqlStr"+sqlData);
        Thread.sleep(Long.MAX_VALUE);
    }
}
