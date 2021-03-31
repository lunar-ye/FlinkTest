package com.hy.flink.ZookeeperTest;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Test1_connectZk {
    private String connectString = "192.168.247.132:2181";
    private int sessionTimeout = 10000;
    private ZooKeeper zkClient;
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for(String child : children) {
                    System.out.println(child);
                }
            }
        });
    }

    // 1.创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/hy","aside".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println(path);
    }

    // 2.获取子节点,并监控节点变化
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for(String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    // 3.判断节点存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/test", false);
        System.out.println(stat != null ? "exist" : "no exist");
    }

}
