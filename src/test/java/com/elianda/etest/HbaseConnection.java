package com.elianda.etest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author tony
 * @describe HbaseConnct
 * @date 2019-07-23
 */
public class HbaseConnection {
    private static Configuration hbaseConf;
    private static Connection connection = null;

    @Test
    public void connection() {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "127.0.0.1");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.setInt("hbase.client.retries.number", 10);
        hbaseConf.setInt("hbase.client.ipc.pool.size", 6);

        try {
            connection = ConnectionFactory.createConnection(hbaseConf);
            Admin admin = connection.getAdmin();
            final List<HRegionInfo> test = admin.getTableRegions(TableName.valueOf("test"));
            System.out.println("获取HBase连接成功");
        } catch (IOException var5) {
            System.out.println("获取HBase连接失败" + var5.getMessage());
        }
    }
}
