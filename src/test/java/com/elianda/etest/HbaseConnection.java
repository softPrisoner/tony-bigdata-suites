package com.elianda.etest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
            if (!admin.tableExists(TableName.valueOf("TRANSACTION"))) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("TRANSACTION"));
                String channelID = "ThirdPay";
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(channelID);
                tableDescriptor.addFamily(columnDescriptor);
                admin.createTable(tableDescriptor);
            } else {
                //防止表被禁止为不可用
                if (admin.isTableDisabled(TableName.valueOf("test"))) {
                    admin.isTableAvailable(TableName.valueOf("test"));
                }
                String channelID = "ThirdPay";
                String merchantNumber = "1000001";
                String transactionNumber = "5000";
                Table transactionTable = connection.getTable(TableName.valueOf("TRANSACTION"));
                Put put = new Put(Bytes.toBytes("testRow1"));
                put.addColumn(Bytes.toBytes(channelID),
                        Bytes.toBytes(merchantNumber)
                        , Bytes.toBytes(transactionNumber));
//                transactionTable.put(put);
                //单条记录
                Get get = new Get(Bytes.toBytes("testRow1"));
                get.addColumn(Bytes.toBytes(channelID), Bytes.toBytes(merchantNumber));
                Result result = transactionTable.get(get);
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    System.out.println
                            ("列簇名:" + Bytes.toString(cell.getFamilyArray()
                                    , cell.getFamilyOffset()
                                    , cell.getFamilyLength()));
                    System.out.println
                            ("唯一标识" + Bytes.toString(cell.getQualifierArray()
                                    , cell.getQualifierOffset()
                                    , cell.getQualifierLength()));
                    System.out.println
                            ("存储的值:" + Bytes.toString(cell.getValueArray()
                                    , cell.getValueOffset()
                                    , cell.getValueLength()));
                }

            }
            System.out.println("获取HBase连接成功");
        } catch (IOException var5) {
            System.out.println("获取HBase连接失败" + var5.getMessage());
        }
    }
}
