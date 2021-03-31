package com.hy.flink.ZookeeperTest;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Test2_DistributeServer {
    private String connectString = "192.168.247.132:2181";
    private int sessionTimeout = 10000;
    private ZooKeeper zkClient;
    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        Test2_DistributeServer server = new Test2_DistributeServer();
        // 1.连接zK集群
        server.getConnect();

        // 2.注册节点
        server.rigist(args[0]);

        // 3.业务逻辑处理
        server.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void rigist(String host) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server",host.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(host + "is online");
    }
}
