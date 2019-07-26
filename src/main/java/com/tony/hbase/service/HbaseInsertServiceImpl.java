package com.tony.hbase.service;

import com.alibaba.fastjson.JSON;
import com.tony.hbase.config.TonyHbaseConf;
import com.tony.hbase.dao.HbaseInsertDao;
import com.tony.hbase.util.HbaseRowKeyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author tony
 * @describe HbaseInsertServiceImpl
 * @date 2019-07-24
 */
public class HbaseInsertServiceImpl implements HbaseInsertService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseInsertService.class);
    private HbaseInsertDao hbaseInsertDao;

    public HbaseInsertServiceImpl() {
        try {
            this.hbaseInsertDao = TonyHbaseConf.getHBaseInsertDao();
        } catch (IOException e) {
            LOGGER.error("Create Insert DAO failed caused by:{}", e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void insertDataFromMap(Map<String, Object> dataMap) throws Exception {
        String family;
        byte[] rowKey = HbaseRowKeyUtils.getTimeRowKey((Long) dataMap.get("seq")
                , (String) dataMap.get("channelID")
                , (Long) dataMap.get("transactionTime"));

        Put put = new Put(rowKey);
        Iterator iterator = dataMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry) iterator.next();
            family = TonyHbaseConf.getFamily(entry.getKey());
            if (Objects.nonNull(family)) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), Bytes.toBytes((String) entry.getValue()));
            }
        }
        this.hbaseInsertDao.insert(put);

    }

    @Override
    public void insertDataFromMapList(List<Map<String, Object>> listData) throws Exception {
        List<Put> putList = new ArrayList<>();
        listData.forEach(data -> {
            String family;
            Put put = new Put(HbaseRowKeyUtils.getTimeRowKey(
                    (Long) data.get("seq")
                    , (String) data.get("channelID")
                    , (Long) data.get("transactionTime")));

            for (Map.Entry<String, Object> entry : data.entrySet()) {
                family = TonyHbaseConf.getFamily(entry.getKey());
                if (null != family) {
                    put.addColumn(Bytes.toBytes(family)
                            , Bytes.toBytes(entry.getKey())
                            , Bytes.toBytes((String) entry.getValue()));
                }
            }
            putList.add(put);
        });
        this.hbaseInsertDao.insert(putList);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void insertDataFromJson(String jsonStr) throws Exception {
        Map<String, String> dataMap = JSON.parseObject(jsonStr, Map.class);
        String family;
        Put put = new Put(HbaseRowKeyUtils.getTimeRowKey(
                Long.parseLong(dataMap.get("seq"))
                , dataMap.get("channelID")
                , Long.parseLong(dataMap.get("transactionTime"))));

        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            family = TonyHbaseConf.getFamily(entry.getKey());
            if (StringUtils.isNotEmpty(family)) {
                put.addColumn(Bytes.toBytes(family)
                        , Bytes.toBytes(entry.getKey())
                        , Bytes.toBytes(entry.getValue()));
            }
        }
        this.hbaseInsertDao.insert(put);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void insertDataFromJsonList(List<String> jsonList) throws Exception {
        List<Put> putList = new ArrayList<>();
        for (String json : jsonList) {
            String family;
            Map<String, String> dataMap = JSON.parseObject(json, Map.class);
            if (dataMap.get("seq") != null) {
                Put put = new Put(HbaseRowKeyUtils
                        .getTimeRowKey(Long.parseLong(dataMap.get("seq"))
                                , dataMap.get("channelID")
                                , Long.parseLong(dataMap.get("transactionTime"))));

                for (Map.Entry entry : dataMap.entrySet()) {
                    family = TonyHbaseConf.getFamily((String) entry.getKey());
                    if (null != family) {
                        put.addColumn(Bytes.toBytes(family)
                                , Bytes.toBytes((String) entry.getKey())
                                , Bytes.toBytes((String) entry.getValue()));
                    }
                }
            }
        }
        hbaseInsertDao.insert(putList);
    }

    @Override
    public void closeTable() {
        this.hbaseInsertDao.close();
    }
}
