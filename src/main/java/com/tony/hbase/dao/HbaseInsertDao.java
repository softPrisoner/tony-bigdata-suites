package com.tony.hbase.dao;

import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * @author tony
 * @describe HbaseInsertDao
 * @date 2019-07-23
 */
public interface HbaseInsertDao {
    /**
     * 插入一条记录
     */
    void insert(Put record) throws Exception;

    /**
     * 插入多条数据
     */
    void insert(List<Put> recordList) throws Exception;

    /**
     * 关闭连接
     */
    void close();
}
