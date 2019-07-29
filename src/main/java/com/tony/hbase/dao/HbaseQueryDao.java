package com.tony.hbase.dao;

import org.apache.hadoop.hbase.client.*;

import java.util.List;
import java.util.Map;

/**
 * @author tony
 * @describe HbaseQueryDao
 * @date 2019-07-23
 */
public interface HbaseQueryDao {
    List<String> getAllTableNames();

    long getCount(String tableName, Scan scan) throws Throwable;

    ResultScanner getResultScanner(String tableName, String family, byte[] startRowKey, byte[] stopRowKey, int limit);

    Map<String, Map<String, String>> getResultScanner(String tableName, String startRowKey, String stopRowKey);

    List<Map<String, String>> getResultScanner(String tableName, String keyword, byte[] startRowKey, byte[] stopRowKey);

    Map<String, Map<String, String>> getResultScannerPrefixFilter(String tableName, String prefix);

    Map<String, String> getRowData(String tableName, byte[] rowKey);

    Result queryBySerialNo(String tableName, byte[] rowKey);

    void putData(String tableName, byte[] rowKey, String familyName, String[] columns, String[] values);

    void putData(String tableName, Put put);

    void putBatchData(String tableName, List<Put> putList);

    void putBatchDataAsync(String tableName, List<Put> putList);

    void setColumnValue(String tableName, String rowKey, String familyName, String column, String value);

    void setColumnValue(String tableName, String familyName, byte[] rowKey, String column, byte[] value);

    boolean deleteColumn(String tableName, String rowKey, String familyName, String column);

    boolean deleteRow(String tableName, String rowKey);

    boolean deleteRow(String tableName, Delete delete);

    boolean batchDeleteRow(String tableName, List<Delete> deleteList);

    void close(Admin admin, ResultScanner scanner, Table table);
}
