package com.tony.hbase.config;

import com.tony.hbase.dao.HbaseInsertDaoImpl;
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
    private static String insertTable = null;
    private static String filePath;

    public TonyHbaseConf(String filePath) {
        this.filePath = filePath;
    }

    public static void init(String path) {
        LOGGER.debug("Init hbase configuration file path:[{}]", filePath);
        filePath = path;
        initFamilyMapping(path);
        initRegionMapping(path);
        initHbaseConf(path);
    }

    public static void init() {
        LOGGER.debug("Init hbase configuration file path:[{}]", filePath);
        initFamilyMapping(filePath);
        initRegionMapping(filePath);
        initHbaseConf(filePath);
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
            props.load(new FileInputStream(new File(filePath + File.separator + "region_mapping_conf.properties")));
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
//        String maxsize = null;
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(new File(filePath + File.separator + "application.properties")));
            nodes = props.getProperty("hbase.nodes");
            port = props.getProperty("hbase.port");
//            maxsize = props.getProperty("hbase.maxsize", "-1");
            insertTable = props.getProperty("hbase.insert.table", "ALL_TRADE_HISTORY");
        } catch (IOException e) {
            LOGGER.error("read config file service.properties failed plesase check your path." +
                    " cause by:{}", e.getMessage());
        }
        hbaseConf = HBaseConfiguration.create();
        //设置zookeeper地址
        hbaseConf.set("hbase.zookeeper.quorum", nodes);
        //设置客户端访问端口
        hbaseConf.set("hbase.zookeeper.property.clientPort", port);
        //default 14
        hbaseConf.setInt("hbase.client.retries.number", 10);
        hbaseConf.setInt("hbase.client.ipc.pool.size", 6);
        hbaseConf.setInt("zookeeper.recovery.retry", 3);
        hbaseConf.setInt("zookeeper.recovery.retry.intervalmill", 1);
        //default 60
        hbaseConf.setInt(" hbase.regionserver.lease.period", 60);
        try {
            connection = ConnectionFactory.createConnection(hbaseConf);
            LOGGER.info("hbase connection success at:[{}]", nodes);
        } catch (IOException e) {
            LOGGER.error("hbase connection failed cause by:{}", e.getMessage());
        }
    }

    public static HbaseInsertDaoImpl getHBaseInsertDao() throws IOException {

        return new HbaseInsertDaoImpl(getTable(insertTable));
    }

    public static Table getTable(String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            if (null == table) {
                LOGGER.warn("get the table from hbase connection failed,please " +
                        "check your tableName:[{}] correctly", tableName);
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
            //找寻匹配的region
            int idx = Math.abs(hash(serialNo.toCharArray())) & 1023;
            return regions[idx].getBytes();
        } catch (Exception e) {
            LOGGER.info("getIdx failed cause by:{}", e.getMessage());
            return null;
        }
    }

    public static int hash(char[] value) {
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
                LOGGER.info("Disconnect with hbase service,if you expect please restart");
            }
        } catch (IOException e) {
            LOGGER.error("Disconnect with the hbase service has happened error caused by:{}", e.getMessage());
        }
    }
}
