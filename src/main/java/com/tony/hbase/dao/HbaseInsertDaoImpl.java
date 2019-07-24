package com.tony.hbase.dao;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author tony
 * @describe HbaseInsertDaoImpl
 * @date 2019-07-23
 */
public class HbaseInsertDaoImpl implements HbaseInsertDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseInsertDaoImpl.class);
    private Table table;

    public HbaseInsertDaoImpl(Table table) {
        this.table = table;
    }

    @Override
    public void insert(Put record) throws Exception {
        Preconditions.checkNotNull(this.table, "The Hbase table should be initialized.");
        this.table.put(record);
        LOGGER.info("Insert one record into hbase table: {}\r\n{} ", table.getName(), record.toJSON());
    }

    @Override
    public void insert(List<Put> recordList) throws Exception {
        Preconditions.checkNotNull(this.table, "The Hbase table should be initialized.");
        this.table.put(recordList);
        LOGGER.info("Insert {} piece of records:[]  into Hbase table:{} ", recordList.size(), table.getName());
    }

    @Override
    public void close() {
        if (!Objects.equal(table, null)) {
            try {
                this.table.close();
            } catch (IOException e) {
                LOGGER.error("Try to disconnect the Hbase table: {} failed caused by:{}", table.getName(), e.getMessage());
            }
        }

    }
}
