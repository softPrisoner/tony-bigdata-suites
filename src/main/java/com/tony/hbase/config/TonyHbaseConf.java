package com.tony.hbase.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author tony
 * @describe HbaseConfiguration
 * @date 2019-07-22
 */
public class TonyHbaseConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(TonyHbaseConf.class);
    private static Connection connection = null;
    private static Map<String, String> familyMap = null;
    private static String[] regions = new String[1024];
    private static Configuration hbaseConf;
    private String insertTable;
    private static String filePath;

    public TonyHbaseConf(String filePath_) {
        this.filePath = filePath_;
    }

    public static void init(String filePath_) {
        LOGGER.debug("Init hbase configuration file path:[{}]", filePath);
        filePath = filePath_;
    }

    private static void initFamilyMapping(String filePath) {
        familyMap = new HashMap<>(16);
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(filePath + File.separator + "family.properties"));
        } catch (IOException e) {
            LOGGER.error("load the family.properties failed cause by:{}", e.getMessage());
        }
        Iterator<String> names = props.stringPropertyNames().iterator();
        while (names.hasNext()) {
            String key = names.next();
            String family = props.getProperty(key).trim();
            familyMap.put(key.toUpperCase(), family);
        }
    }

    private static void initRegionMapping(String filePath) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(new File(filePath)));
        } catch (FileNotFoundException e) {
            LOGGER.error("check your family.properties filePath error happen cause by{}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("read your family.properties filePath error. cause by{}", e.getMessage());
        }
        for (int i = 0; i < regions.length; ++i) {
            regions[i] = props.getProperty(i + "");
        }
    }

    private static void initHbaseConf(String filePath) {
        String nodes = null;
        String port = null;
        String maxsize = null;

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(new File(filePath + File.separator + "service.properties")));
            nodes = props.getProperty("hbase.nodes");
            port = props.getProperty("port");
            maxsize = props.getProperty("hbase.maxsize", "-1");
            String insertTable = props.getProperty("hbase.insertTable", "ALL_TRADE_HISTORY");
        } catch (IOException e) {
            LOGGER.error("read config file service.properties failed plesase check your path. cause by"
                    , e.getMessage());
        }
        hbaseConf = HBaseConfiguration.create();
        //设置zookeeper地址
        hbaseConf.set("hbase.zookeeper.quorum", nodes);
        //设置客户端访问端口
        hbaseConf.set("hbase.zookeeper.property.clientPort", port);
        hbaseConf.setInt("hbase.client.retries.number", 10);
        hbaseConf.setInt("hbase.client.ipc.pool.size", 6);
        try {
            connection = ConnectionFactory.createConnection(hbaseConf);
            LOGGER.info("hbase connection success at");
        } catch (IOException e) {
            LOGGER.error("hbase connection failed cause by:{}", e.getMessage());
        }
    }

    public static Table getTable(String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            if (null == table) {
                LOGGER.warn("get the table from hbase connection failed,please check your tableName:[{}] correctly", tableName);
            }
            return table;

        } catch (IOException e) {
            LOGGER.error("some inner error has happened when get the " +
                    "table name from hbase service. cause by{}", e.getMessage());
            e.printStackTrace();
        }
        return table;
    }

    public static boolean isHasField(String field) {
        return familyMap.containsKey(field.toUpperCase());
    }

    public static String getFamily(String key) {
        return familyMap.get(key);
    }

    public static Configuration getHbaseConf() {
        return hbaseConf;
    }

    public String getInsertTable() {
        return insertTable;
    }

    public static byte[] getIdx(long accNo) {
        try {
            int idx = (int) (accNo & 1023L);
            return regions[idx].getBytes();
        } catch (Exception e) {
            LOGGER.info("getIdx failed cause by:{}", e.getMessage());
            return null;
        }
    }


    public static byte[] getIdx(String serialNo) {
        try {
            int idx = Math.abs(hashCode(serialNo.toCharArray())) & 1023;
            return regions[idx].getBytes();
        } catch (Exception e) {
            LOGGER.info("getIdx failed cause by:{}", e.getMessage());
            return null;
        }
    }

    public static int hashCode(char[] value) {
        int h = 0;
        if (value.length > 0) {
            for (char c : value) {
                h = 37 * h + c;
            }
        }
        return h;
    }

    public static void closeConnection() {
        try {
            if (null != connection) {
                connection.close();
                LOGGER.info("disconnect with hbase service,if you expect please restart");
            }
        } catch (IOException e) {
            LOGGER.error("disconnecting the hbase service has happened error cause by:{}", e.getMessage());
            e.printStackTrace();
        }
    }
}
