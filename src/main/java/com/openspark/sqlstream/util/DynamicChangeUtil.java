package com.openspark.sqlstream.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class DynamicChangeUtil implements Watcher {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    public final static String[] zkData = {""};

    public static void putDataInZk(CuratorFramework client, String sql, String group, String sqlPath) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(ConfigConstrant.ZOOKEPER_ADRESS, 5000, new DynamicChangeUtil());
        countDownLatch.await();
        String zkPath = "/" + group + "/" + sqlPath;
        Stat groupExists = zooKeeper.exists("/" + group, new DynamicChangeUtil());
        if (groupExists == null) {
            zooKeeper.create("/" + group, null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        Stat pathExists = zooKeeper.exists("/" + group + "/" + sqlPath, new DynamicChangeUtil());
        if (pathExists == null) {
            zooKeeper.create("/" + group + "/" + sqlPath, sql.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zooKeeper.close();
            setDataNode(client, "/" + group + "/" + sqlPath, sql);
        }
    }

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
                ChildData data = event.getData();
                if (data != null) {
                    switch (event.getType()) {
                        case NODE_ADDED:
                            System.out.println("NODE_ADDED");
                            zkData[0] = new String(data.getData());
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED");
                            zkData[0] = new String(data.getData());
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED");
                            zkData[0] = new String(data.getData());
                            break;

                        default:
                            break;
                    }
                } else {
                    System.out.println("data is null : " + event.getType());
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

    private static void setDataNode(CuratorFramework client, String path, String message) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            throw new RuntimeException("zk的路径不存在" + path);
        }
        client.setData().forPath(path, message.getBytes());
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
}
