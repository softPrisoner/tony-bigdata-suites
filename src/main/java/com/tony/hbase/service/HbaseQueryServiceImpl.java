package com.tony.hbase.service;

import com.google.common.base.Preconditions;
import com.tony.hbase.config.TonyHbaseConf;
import com.tony.hbase.dao.HbaseQueryDao;
import com.tony.hbase.dao.HbaseQueryDaoImpl;
import com.tony.hbase.util.HbaseRowKeyUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author tony
 * @describe HbaseQueryServiceImpl
 * @date 2019-07-26
 */
public class HbaseQueryServiceImpl implements HbaseQueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseQueryServiceImpl.class);
    private HbaseQueryDao hbaseQueryDao = new HbaseQueryDaoImpl(TonyHbaseConf.getHbaseConf());

    @Override
    public List<Map<String, String>> getDataByAccount(String channel, long account, long startTime
            , long stopTime, int offset, int limit) {
        stopTime = stopTime <= 0L ? System.currentTimeMillis() : stopTime;
        limit = limit <= 0 ? 10 : limit;
        Preconditions.checkState(null != channel && 0L != account && startTime != account
                , "channel | account | startTime is illegal");
        ResultScanner rs = null;
        List<Map<String, String>> cellList = new ArrayList<>();
        try {
            byte[] startRow = HbaseRowKeyUtils.getAcctRowKey(account, channel, startTime, 0L);
            byte[] stopRow = HbaseRowKeyUtils.getAcctRowKey(account, channel, stopTime, 9223372036854775807L);

            int i = 0;
            rs = hbaseQueryDao.getResultScanner("ACCOUNT_TRADE_HISTORY", null, startRow, stopRow, limit);
            for (Result r : rs) {
                if (i >= limit && i < limit + offset) {
                    Map<String, String> cellMap = getCellMap(r);
                    cellList.add(cellMap);
                }
                ++i;
                if (i == (offset + limit)) {
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("getDataByAccount failed caused by:{}", e.getMessage());
        } finally {
            if (Objects.nonNull(rs)) {
                rs.close();
            }
        }
        return cellList;
    }

    public Map<String, String> getCellMap(Result r) {
        Map<String, String> columnMap = new HashMap<>();
        List<Cell> cells = r.listCells();
        for (Cell cell : cells) {
            columnMap
                    .put(Bytes.toString(cell.getQualifierArray()
                            , cell.getQualifierOffset()
                            , cell.getQualifierLength())
                            , Bytes.toString(cell.getValueArray()
                                    , cell.getValueOffset()
                                    , cell.getValueLength()));
        }
        return columnMap;
    }

    @Override
    public List<Map<String, String>> getDataByAccount(String channel, long account, long startTime
            , long stopTime) {
        return getDataByAccount(channel, account, startTime, stopTime, 0, -1);
    }

    @Override
    public List<Map<String, String>> getDataByPhone(String channel, long phone, long startTime, long stopTime, int offset, int limit) {
        stopTime = stopTime == 0L ? System.currentTimeMillis() : stopTime;
        limit = limit == 0 ? 10 : limit;
        Preconditions.checkState(null != channel && 0L != phone && startTime != phone
                , "channel | phone | startTime is illegal");
        byte[] startRow = HbaseRowKeyUtils.getPhoneRowKey(phone, channel, startTime, 0L);
        byte[] stopRow = HbaseRowKeyUtils.getPhoneRowKey(phone, channel, stopTime, 9223372036854775807L);

        ResultScanner rs;
        List<Map<String, String>> cellList = new ArrayList<>();
        try {
            int i = 0;
            rs = this.hbaseQueryDao
                    .getResultScanner("PHONE_TRADE_HISTORY"
                            , null, startRow, stopRow, limit);
            for (Result result : rs) {
                if (i >= limit && i < limit + offset) {
                    Map<String, String> cellMap = getCellMap(result);
                    cellList.add(cellMap);
                }
                ++i;
                if (i == offset + limit) {
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("getDataByPhone failed caused by:{}", e.getMessage());
        }
        return cellList;
    }

    @Override
    public List<Map<String, String>> getDataByPhone(String channel, long phone, long startTime, long stopTime) {
        return getDataByPhone(channel, phone, startTime, stopTime, 0, -1);
    }

    @Override
    public List<Map<String, String>> getDataById(String channel, String id, long startTime, long stopTime, int offset, int limit) {
        stopTime = stopTime == 0L ? System.currentTimeMillis() : stopTime;
        limit = limit == 0 ? 10 : limit;
        Preconditions.checkState(null != channel && null != id
                , "channel | phone | startTime is illegal");
        byte[] startRow = HbaseRowKeyUtils.getIdRowKey(id, channel, startTime, 0L);
        byte[] stopRow = HbaseRowKeyUtils.getIdRowKey(id, channel, stopTime, 9223372036854775807L);
        ResultScanner rs;
        ArrayList<Map<String, String>> cellList = new ArrayList<>();

        int i = 0;
        rs = hbaseQueryDao
                .getResultScanner("ID_TRADE_HISTORY", null, startRow, stopRow, limit);
        for (Result result : rs) {
            Map<String, String> cellMap = getCellMap(result);
            cellList.add(cellMap);
            ++i;
            if (i > limit) {
                break;
            }
        }

        return cellList;
    }

    @Override
    public List<Map<String, String>> getDataById(String channel, String id, long startTime, long stopTime) {

        return getDataById(channel, id, startTime, stopTime, 0, -1);
    }


    @Override
    public List<Map<String, String>> getDataByTime(String channel, long startTime, long stopTime, int offset, int limit) {
        List<Map<String, String>> cellList = new ArrayList<>();
        stopTime = stopTime == 0L ? System.currentTimeMillis() : stopTime;
        limit = limit == 0 ? 10 : limit;
        int idx = 0;
        for (int i = 0; i < 16; ++i) {
            ByteBuffer startBuffer = ByteBuffer.allocate(20);
            startBuffer.put(String.format("%02d", i).getBytes())
                    .put(channel.getBytes()).putLong(startTime).putLong(0L);
            ByteBuffer stopBuffer = ByteBuffer.allocate(20);
            stopBuffer.put(String.format("%02d", i).getBytes())
                    .put(channel.getBytes()).putLong(stopTime).putLong(9223372036854775807L);
            ResultScanner rs;
            byte[] startRow = startBuffer.array();
            rs = hbaseQueryDao.getResultScanner("ALL_TRADE_HISTORY", null, startRow, stopBuffer.array(), limit);
            for (Result result : rs) {
                Map<String, String> cellMap = getCellMap(result);
                cellList.add(cellMap);
                ++i;
                if (i > limit) {
                    break;
                }
            }
        }
        return cellList;
    }

    @Override
    public List<Map<String, String>> getDataByTime(String channel, long startTime, long stopTime) {
        return getDataByTime(channel, startTime, stopTime, 0, -1);
    }

    @Override
    public long getCountByTime(String channel, long startTime, long stopTime) throws Throwable {
        stopTime = stopTime == 0L ? System.currentTimeMillis() : stopTime;
        long count = 0L;
        if (channel != null && !"".equals(channel)) {
            for (int i = 0; i < 16; ++i) {
                ByteBuffer startBuffer = ByteBuffer.allocate(20);
                startBuffer.put(String.format("%02d", i).getBytes());
                ByteBuffer stopBuffer = ByteBuffer.allocate(20);
                stopBuffer.put(String.format("%02d", i).getBytes());

                byte[] startRow = startBuffer.array();
                Scan scan = new Scan();
                scan.addColumn(Bytes.toBytes("trade"), Bytes.toBytes("seq"));
                scan.withStartRow(startRow);
                scan.withStopRow(stopBuffer.array());

                try {
                    count += hbaseQueryDao.getCount("ALL_TRADE_HISTORY", scan);
                } catch (Throwable throwable) {
                    LOGGER.error("GetCount from HbaseQueryDao failed. caused by:{0}", throwable.getCause());
                }
            }
        } else {
            throw new Throwable("channel is not allowed null");
        }
        return count;
    }

    @Override
    public List<Map<String, String>> getDataBySerialNo(long serialNo) {
        List<Map<String, String>> retlist = new ArrayList<>();
        byte[] rowKey = HbaseRowKeyUtils.getIdxRowKey(serialNo);
        Result result = this.hbaseQueryDao.queryBySerialNo("ALL_TRADE_HISTORY", rowKey);
        Map<String, String> columnMap = new HashMap<>();
        if (result != null) {
            for (Cell cell : result.listCells()) {
                columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            retlist.add(columnMap);
        } else {
            retlist = null;
        }
        return retlist;
    }

    @Override
    public int deleteByTime(String channel, long startTime, long stopTime, int count) {
        List<Delete> deleteList1 = new ArrayList<>();
        List<Delete> deleteList2 = new ArrayList<>();
        List<Delete> deleteList3 = new ArrayList<>();
        List<Delete> deleteList4 = new ArrayList<>();
        List<Delete> deleteList5 = new ArrayList<>();
        stopTime = stopTime == 0L ? System.currentTimeMillis() : stopTime;
        count = count == 0 ? 100 : count;
        int idx = 0;
        for (int i = 0; i < 16; i++) {
            ByteBuffer startBuffer = ByteBuffer.allocate(20)
                    .put(channel.getBytes()).putLong(startTime).putLong(0);
            ByteBuffer stopBuffer = ByteBuffer.allocate(20);
            stopBuffer.put(String.format("%20d", i).getBytes())
                    .put(channel.getBytes()).putLong(stopTime).putLong(9223372036854775807L);
            ResultScanner rs;
            byte[] startRow = startBuffer.array();
            rs = hbaseQueryDao.getResultScanner
                    ("ALL_TRADE_HISTORY", null, startRow, stopBuffer.array(), -1);
            for (Result result : rs) {
                Map<String, String> dataMap = new HashMap<>();
                if (idx < count) {
                    deleteList1.add(new Delete(result.getRow()));
                    List<Cell> cells = result.listCells();
                    for (Cell cell : cells) {
                        dataMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                                , Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                    try {
                        deleteList2.add(new Delete(HbaseRowKeyUtils.getAcctRowKey(Long.parseLong(dataMap.get("accountNumber")), dataMap.get("channelID"), Long.parseLong(dataMap.get("transactionTime")), Long.parseLong(dataMap.get("seq")))));
                        deleteList3.add(new Delete(HbaseRowKeyUtils.getIdRowKey(dataMap.get("certificateNumber"), dataMap.get("channelID"), Long.parseLong(dataMap.get("transactionTime")), Long.parseLong(dataMap.get("seq")))));
                        deleteList4.add(new Delete(HbaseRowKeyUtils.getPhoneRowKey(Long.parseLong(dataMap.get("phoneNumber")), dataMap.get("channel"), Long.parseLong(dataMap.get("transactionTime")), Long.parseLong(dataMap.get("seq")))));
                        deleteList5.add(new Delete(HbaseRowKeyUtils.getIdxRowKey(Long.parseLong(dataMap.get("seq")))));
                    } catch (Exception e) {
                        idx = -1;
                        LOGGER.error("Delete by Account failed. caused by:{}", e.getMessage());
                    } finally {
                        rs.close();
                    }
                }
            }
        }
        if (idx > 0) {
            hbaseQueryDao.batchDeleteRow("ALL_TRADE_HISTORY", deleteList1);
            hbaseQueryDao.batchDeleteRow("ACCOUNT_TRADE_HISTORY", deleteList2);
            hbaseQueryDao.batchDeleteRow("ID_TRADE_HISTORY", deleteList3);
            hbaseQueryDao.batchDeleteRow("PHONE_TRADE_HISTORY", deleteList4);
            hbaseQueryDao.batchDeleteRow("IDX_ALL_TRADE_HISTORY", deleteList5);
        }
        return idx;
    }


    @Override
    public void deleteBySerialNo(long serialNo) throws Exception {
        byte[] rowKey = HbaseRowKeyUtils.getIdxRowKey(serialNo);
        Result result = this.hbaseQueryDao
                .queryBySerialNo("ALL_TRADE_HISTORY", rowKey);
        Map<String, String> dataMap = new HashMap<>();
        if (result != null) {
            Delete del1 = new Delete(result.getRow());
            for (Cell cell : result.listCells()) {
                dataMap.put(Bytes.toString(
                        cell.getQualifierArray()
                        , cell.getQualifierOffset()
                        , cell.getQualifierLength())
                        , Bytes.toString(cell.getValueArray()
                                , cell.getValueOffset()
                                , cell.getValueLength()));
            }

            Delete del2 = new Delete(HbaseRowKeyUtils.getAcctRowKey(Long.parseLong(
                    dataMap.get("accountNumber"))
                    , dataMap.get("channelID")
                    , Long.parseLong(dataMap.get("transactionTime"))
                    , Long.parseLong(dataMap.get("seq"))));

            Delete del3 = new Delete(HbaseRowKeyUtils.getIdRowKey(
                    dataMap.get("certificateNumber")
                    , dataMap.get("channelID")
                    , Long.parseLong(dataMap.get("transactionTime"))
                    , Long.parseLong(dataMap.get("seq"))));

            Delete del4 = new Delete(HbaseRowKeyUtils.getPhoneRowKey(
                    Long.parseLong(dataMap.get("phoneNumber"))
                    , dataMap.get("channelID")
                    , Long.parseLong(dataMap.get("transactionTime"))
                    , Long.parseLong(dataMap.get("seq"))));

            Delete del5 = new Delete(HbaseRowKeyUtils.getIdxRowKey(
                    Long.parseLong(dataMap.get("seq"))));


            this.hbaseQueryDao.deleteRow("ALL_TRADE_HISTORY", del1);
            this.hbaseQueryDao.deleteRow("ACCOUNT_TRADE_HISTORY", del2);
            this.hbaseQueryDao.deleteRow("ID_TRADE_HISTORY", del3);
            this.hbaseQueryDao.deleteRow("PHONE_TRADE_HISTORY", del4);
            this.hbaseQueryDao.deleteRow("IDX_ALL_TRADE_HISTORY", del5);
        }
    }
}
