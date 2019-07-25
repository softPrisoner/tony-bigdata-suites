package com.tony.hbase.service;

import java.util.List;
import java.util.Map;

/**
 * @author tony
 * @describe HbaseInsertService
 * @date 2019-07-25
 */
public interface HbaseInsertService {
    /**
     * 通过map插入数据
     */
    void insertDataFromMap(Map<String, Object> dataMap) throws Exception;

    /**
     * 批量插入数据
     */
    void insertDataFromMapList(List<Map<String, Object>> listData) throws Exception;

    /**
     * 插入json格式数据
     */
    void insertDaoFromJson(String json) throws Exception;

    /**
     * 插入列表序列
     */

    void insertDataFromJsonList(List<String> jsonList) throws Exception;

    /**
     * 断开表连接
     */
    void closeTable();
}
