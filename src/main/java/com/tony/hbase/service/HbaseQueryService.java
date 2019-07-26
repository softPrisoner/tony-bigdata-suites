package com.tony.hbase.service;

import java.util.List;
import java.util.Map;

/**
 * @author tony
 * @describe HbaseQueryService
 * @date 2019-07-26
 */
public interface HbaseQueryService {

    List<Map<String, String>> getDataByAccount(String channel, long account, long startTime, long endTime, int offset, int limit);

    List<Map<String, String>> getDataByAccount(String channel, long account, long startTime, long stopTime);

    List<Map<String, String>> getDataByPhone(String channel, long phone, long startTime, long stopTime, int offset, int limit);

    List<Map<String, String>> getDataByPhone(String channel, long phone, long startTime, long stopTime);

    List<Map<String, String>> getDataById(String channel, String id, long startTime, long endTime, int offset, long limit);

    List<Map<String, String>> getDataById(String channel, String id, long startTime, long endTime);

    List<Map<String, String>> getDataByTime(String channel, long startTime, long endTime, int offset, long limit);

    List<Map<String, String>> getDataByTime(String channel, long startTime, long endTime);

    long getCountByTime(String channel, long startTime, long endTime);

    int deleteByTime(String channel, long startTime, long endTime, int count);

    List<Map<String, String>> getDataBySerialNo(long serialNo);

    void deleteBySerialNo(long serialNo);


}
