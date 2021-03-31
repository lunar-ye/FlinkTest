package com.hy.flink.ZookeeperTest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test2_DistributeClient {
    //create -e -s "/servers/server" "aaaa"
    private String connectString = "192.168.247.132:2181";
    private int sessionTimeout = 10000;
    private ZooKeeper zkClient;
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Test2_DistributeClient server = new Test2_DistributeClient();
        // 1.获取zk连接
        server.getConnect();
        // 2.注册监听
        server.getChildren();
        // 3.业务逻辑处理
        server.business();
    }

    private void getChildren() throws KeeperException, InterruptedException {
        List<String> list = zkClient.getChildren("/servers", true);
        ArrayList<String> hosts = new ArrayList<>();
        for(String child : list) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
            System.out.println(hosts);

        }
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
