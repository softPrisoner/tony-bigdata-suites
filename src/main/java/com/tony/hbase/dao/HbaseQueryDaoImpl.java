package com.tony.hbase.dao;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * @author tony
 * @describe HbaseQueryDaoImpl
 * @date 2019-07-23
 */
public class HbaseQueryDaoImpl implements HbaseQueryDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseInsertDaoImpl.class);
    private Configuration hbaseConf;
    private Connection connection = null;
    private final ColumnInterpreter<Long, Long, HBaseProtos.EmptyMsg,
            HBaseProtos.LongMsg, HBaseProtos.LongMsg> ci = new LongColumnInterpreter();
    private static final String TABLE_PREFIX = "IDX_";

    public HbaseQueryDaoImpl(Configuration hbaseConf) {
        Preconditions.checkNotNull(hbaseConf, "Check your hbaseConf. Make sure it's not null.");
        this.hbaseConf = hbaseConf;
        try {
            this.connection = ConnectionFactory.createConnection(hbaseConf);
            Date nowDate = new Date();
            String now = DateFormatUtils.format(nowDate, "yyyy-MM-dd hh:mm:ss");
            LOGGER.info("Connect to the Hbase server at {}", now);
        } catch (IOException e) {
            LOGGER.info("Try connect to the hbase server failed caused by:{}", e.getMessage());
        }
    }

    /**
     * 获取对应表名的hbase表
     */
    private Table getTable(String tableName) {
        Preconditions.checkArgument("".equals(tableName), "Check your hbaseConf . Make sure it's legal.");
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            if (Objects.isNull(table)) {
                LOGGER.warn("Could't find the table .Please check the tableName:{}", tableName);
            }
            return table;
        } catch (IOException e) {
            LOGGER.error("Connect to the table: [{}] failed cause by:{}", tableName, e.getMessage());
        }
        return null;
    }

    /**
     * 获取所有表名
     */
    public List<String> getAllTableNames() {
        List<String> namesList = new ArrayList<>();
        Admin admin = null;
        try {
            admin = this.connection.getAdmin();
            //列出所有表名
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                String nameAsString = tableName.getNameAsString(); //获取表名并转换成字符串
                namesList.add(nameAsString);
            }
        } catch (IOException e) {
            LOGGER.error("Get the Admin failed cause by:{}", e.getMessage());
        } finally {
            close(admin, null, null);
        }
        return namesList;
    }

    /**
     * 获取hbase相应表下总记录数
     * 强调为什么使用long不适用int 数据量过大int支持数量2^32-1
     */
    public long getCount(String tableName, Scan scan) throws Throwable {
        AggregationClient client = new AggregationClient(this.hbaseConf);
        Table table = getTable(tableName);
        Preconditions.checkNotNull(table, "Check table is not null.");
        return client.rowCount(table, this.ci, scan);
    }

    /**
     * 批量查询,通过列簇
     */
    public ResultScanner getResultScanner(String tableName, String family
            , byte[] startRowKey, byte[] stopRowKey, int limit) {
        Scan scan = new Scan();
        if (!Objects.isNull(family)) {
            scan.addFamily(Bytes.toBytes(family));
        }
        if (limit > 0) {
            PageFilter pageFilter = new PageFilter(limit);
            scan.setFilter(pageFilter);
        }
        if (!Objects.isNull(startRowKey)) {
            scan.withStartRow(startRowKey).withStopRow(stopRowKey);
        }
        if (!Objects.isNull(stopRowKey)) {
            scan.withStopRow(stopRowKey);
        }
        Table table = null;
        ResultScanner scanner = null;
        try {
            table = getTable(tableName);
            Preconditions.checkNotNull(table, "Please check table is not null");
            scanner = table.getScanner(scan);
        } catch (Exception e) {
            LOGGER.error("Visit the table:{} failed cause by{}", tableName, e.getMessage());
        } finally {
            close(null, scanner, table);
        }
        return scanner;
    }

    /**
     * 获取结果扫描器,主要时支持String的Scan配置
     */
    public Map<String, Map<String, String>> getResultScanner(String tableName
            , String startRowKey, String stopRowKey) {
        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(startRowKey) && StringUtils.isNotBlank(stopRowKey)) {
            scan.withStartRow(startRowKey.getBytes()).withStopRow(stopRowKey.getBytes());
        }
        return queryData(tableName, scan);

    }

    /**
     * 获取结果扫描器
     */
    public List<Map<String, String>> getResultScanner(
            String tableName, String keyword, byte[] startRowKey, byte[] stopRowKey) {
        List<Map<String, String>> resultList = new ArrayList<>();
        Scan scan = new Scan();
        if (Objects.nonNull(keyword)) {
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL
                    , new SubstringComparator(keyword));
            scan.setFilter(filter);
        }
        ResultScanner scanner = null;
        Table table = getTable(tableName);
        scan.withStartRow(startRowKey).withStopRow(stopRowKey);
        try {
            Preconditions.checkNotNull(table, "Please check table is not null");
            scanner = table.getScanner(scan);

            for (Result result : scanner) {
                Map<String, String> columnMap = new HashMap<>();
                List<Cell> cells = result.listCells();
                cells.stream().parallel().forEach(cell ->
                        columnMap.put(Bytes.toString(cell.getQualifierArray()
                                , cell.getQualifierOffset()
                                , cell.getQualifierLength())
                                , Bytes.toString(cell.getValueArray()
                                        , cell.getValueOffset()
                                        , cell.getValueOffset())));
                resultList.add(columnMap);
            }
        } catch (IOException e) {
            assert table != null;
            LOGGER.error("Get scanner from table :{} failed cause by:{}", table.getName(), e.getMessage());
        } finally {
            this.close(null, scanner, table);
        }
        return resultList;
    }

    /**
     * 通过前缀获取扫描结果
     * tips:有点像redis
     */
    public Map<String, Map<String, String>> getResultScannerPrefixFilter(String tableName, String prefix) {
        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(prefix) && StringUtils.isNotBlank(prefix)) {
            Filter filter = new PrefixFilter(Bytes.toBytes(prefix));
            scan.setFilter(filter);
        }
        return this.queryData(tableName, scan);
    }

    private Map<String, Map<String, String>> queryData(String tableName, Scan scan) {
        Map<String, Map<String, String>> resultMap = new HashMap<>(1024);
        ResultScanner resultScanner = null;
        Table table = null;

        try {
            table = this.getTable(tableName);
            Preconditions.checkNotNull(table, "Please check table is not null");
            resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                Map<String, String> columnMap = new HashMap<>();
                String rowKey = null;
                Cell cell;
                for (Cell value : result.listCells()) {
                    cell = value;
                    columnMap.put(Bytes.toString(cell.getQualifierArray()
                            , cell.getQualifierOffset()
                            , cell.getQualifierLength())
                            , Bytes.toString(cell.getValueArray()
                                    , cell.getValueOffset()
                                    , cell.getValueLength()));
                    if (StringUtils.isEmpty(rowKey)) {
                        rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    }
                }
                if (StringUtils.isNotEmpty(rowKey)) {
                    resultMap.put(rowKey, columnMap);
                }
            }
        } catch (IOException e) {
            assert table != null;
            LOGGER.error("Get scanner from table :{} failed cause by:{}", table.getName(), e.getMessage());
        } finally {
            this.close(null, resultScanner, table);
        }
        return resultMap;
    }

    public Map<String, String> getRowData(String tableName, byte[] rowKey) {
        Map<String, String> resultMap = new HashMap<>();
        Get get = new Get(rowKey);
        Table table;
        table = this.getTable(tableName);
        try {
            Preconditions.checkNotNull(table, "Please check table is not null");
            Result rowResult = table.get(get);
            if (Objects.nonNull(rowResult) && !rowResult.isEmpty()) {
                for (Cell cell : rowResult.listCells()) {
                    resultMap.put(Bytes.toString(cell.getQualifierArray()
                            , cell.getQualifierOffset()
                            , cell.getQualifierLength())
                            , Bytes.toString(cell.getValueArray()
                                    , cell.getValueOffset()
                                    , cell.getValueLength()));
                }
            }
        } catch (IOException e) {
            assert table != null;
            LOGGER.error("Get data from table :{} by rowKey :{}  failed cause by:{}", table.getName(), rowKey, e.getMessage());
        } finally {
            close(null, null, table);
        }
        return resultMap;

    }

    public Result queryBySerialNo(String tableName, byte[] rowKey) {
        Map<String, byte[]> keyMap = new HashMap<>(16);
        Get get;
        Table table = null;
        Result result = null;
        try {
            get = new Get(rowKey);
            table = getTable(TABLE_PREFIX + tableName);
            Preconditions.checkNotNull(table, "Please check table is not null");
            result = table.get(get);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                keyMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        , cell.getValueArray());
            }
        } catch (IOException e) {
            LOGGER.error("Get One rowData failed cause by:{}", e.getMessage());
        } finally {
            close(null, null, table);
        }
        if (null == keyMap.get("rowId")) {
            return null;
        } else {
            get = new Get(keyMap.get("rowId"));
            table = getTable(tableName);
            try {
                Preconditions.checkNotNull(table, "Please check table is not null");
                result = table.get(get);
            } catch (IOException e) {
                LOGGER.error("Get One rowData failed cause by:{}", e.getMessage());
            }
            return result;
        }
    }

    public void putData(String tableName, byte[] rowKey, String familyName, String[] columns, String[] values) {
        Table table = null;
        try {
            table = getTable(tableName);
            Preconditions.checkNotNull(table, "Please check table is not null");
            putData(table, rowKey, tableName, familyName, columns, values);
        } catch (Exception e) {
            LOGGER.error("Put Data into Hbase failed caused by:" + e.getMessage());
        } finally {
            close(null, null, table);
        }

    }

    public void putData(String tableName, Put put) {
        Table table;
        try {
            table = getTable(tableName);
            Preconditions.checkNotNull(table, "Please check table is not null");
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("Put Data into Hbase failed caused by:" + e.getMessage());
        }


    }

    /**
     * 批量向表中插入记录
     */
    public void putBatchData(String tableName, List<Put> putList) {
        Table table = null;
        try {
            table = getTable(tableName);
            Preconditions.checkNotNull(table, "Please check table is not null");
            table.put(putList);
        } catch (IOException e) {
            LOGGER.error("Batch put records into table [{}] failed cause by:{}", tableName, e.getMessage());
        } finally {
            close(null, null, table);
        }
    }

    /**
     * 异步批量插入数据
     */
    public void putBatchDataAsync(String tableName, List<Put> putList) {
        BufferedMutator table = null;
        try {
            table = this.connection.getBufferedMutator(TableName.valueOf(tableName));
            table.mutate(putList);
        } catch (IOException e) {
            LOGGER.error("get BufferedMutator field failed caused by:{}", e.getMessage());
        } finally {
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    LOGGER.error("client try to close table failed caused by:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 批量插入数据
     */
    private void putData(Table table, byte[] rowKey, String tableName,
                         String familyName, String[] columns, String[] values) {
        Put put = new Put(rowKey);
        if (null != columns && null != values && columns.length == values.length) {
            for (int idx = 0; idx < columns.length; ++idx) {
                if (null == columns[idx] || null == values[idx]) {
                    LOGGER.error("columns | values is null.please check data where idx:" + idx);
                    continue;
                }
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columns[idx]), Bytes.toBytes(values[idx]));
            }
        }
        try {
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("put | update operator with the table of {} failed caused by:{}", tableName, e.getMessage());
        } finally {
            close(null, null, table);
        }
    }

    public void setColumnValue(String tableName, String rowKey, String familyName, String column, String value) {
        Table table;
        table = this.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            Preconditions.checkNotNull(table, "Please check table is not null");
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("set value for row-family failed caused by:{}", e.getMessage());
        } finally {
            close(null, null, table);
        }
    }

    public void setColumnValue(String tableName, String familyName, byte[] rowKey, String column, byte[] value) {
        Table table;
        table = getTable(tableName);
        Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), value);
        try {
            Preconditions.checkNotNull(table, "Please check table is not null");
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("Update value for row-family failed caused by:{}", e.getMessage());
        } finally {
            close(null, null, table);
        }
    }

    /**
     * 删除列必须获取Admin权限
     */
    public boolean deleteColumn(String tableName, String rowKey, String familyName, String column) {
        Table table = null;
        Admin admin = null;
        try {
            admin = this.connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getTable(tableName);
                Preconditions.checkNotNull(table, "Please check table is not null");
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column));
                table.delete(delete);
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("Delete Column for row-family failed caused by:{}", e.getMessage());
        } finally {
            close(admin, null, table);
        }
        return false;
    }

    public boolean deleteRow(String tableName, String rowKey) {
        Table table = null;
        Admin admin = null;
        try {
            admin = this.connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getTable(tableName);
                Preconditions.checkNotNull(table, "Please check table is not null");
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                table.delete(delete);
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("delete rows for rowKey:[{}] failed caused by:{}", rowKey, e.getMessage());
        } finally {
            close(admin, null, table);
        }
        return false;
    }

    public boolean deleteRow(String tableName, Delete delete) {
        Table table = null;
        Admin admin = null;
        try {
            admin = this.connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getTable(tableName);
                Preconditions.checkNotNull(table, "Please check table is not null");
                table.delete(delete);
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("Delete Row:{} failed caused by:{}", Bytes.toString(delete.getRow()), e.getMessage());
        } finally {
            close(admin, null, table);
        }
        return false;
    }

    /**
     * 批量删除行
     */
    public boolean batchDeleteRow(String tableName, List<Delete> deleteList) {
        Table table = null;
        Admin admin = null;
        try {
            admin = this.connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getTable(tableName);
                Preconditions.checkNotNull(table, "Please check table is not null");
                table.delete(deleteList);
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("Batch Delete list by Delete failed caused by:{}", e.getMessage());
        } finally {
            close(admin, null, table);
        }
        return false;
    }

    public void close(Admin admin, ResultScanner scanner, Table table) {

        if (!Objects.isNull(admin)) {
            try {
                admin.close();
                LOGGER.debug("Admin [{}] is be closed", admin.getClusterStatus());
            } catch (IOException e) {
                LOGGER.error("Try to disconnect Hbase Admin failed. cause by:{}", e.getMessage());
            }
        }
        if (!Objects.isNull(scanner)) {
            scanner.close();
            LOGGER.debug("ResultScanner is be closed. metrics:[{}]", scanner.getScanMetrics());
        }

        if (!Objects.isNull(table)) {
            try {
                table.close();
            } catch (IOException e) {
                LOGGER.error("Try to disconnect Table :[{}] failed. cause by:{}", table.getName(), e.getMessage());
            }
        }

    }


}


