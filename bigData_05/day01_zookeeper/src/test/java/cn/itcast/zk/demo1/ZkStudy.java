package cn.itcast.zk.demo1;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.Test;

public class ZkStudy {
    /*
    * 创建临时节点
    */
    @Test
    public void createNode2() throws Exception {

        /*RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 3000, 3000, retryPolicy);
        client.start();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello5/world");
        Thread.sleep(5000);
        client.close();*/

        String connectStr = "node01:2181,node02:2181,node03:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 3000, 3000, retryPolicy);
        client.start();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hahahah01/lalala02");
        client.close();
    }
}
