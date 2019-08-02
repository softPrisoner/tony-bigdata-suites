package com.tony.hbase.model;

import java.util.List;
import java.util.Map;

/**
 * @author tony
 * @describe TransactionDO
 * @date 2019-07-24
 */
public class TransactionDO {
    private List<String> data;
    private int pageNo;
    private int limit;
    private long startTime;
    private long stopTime;
    private long phone;
    private long account;
    private String id;
    private String channel;
    private Map<String, byte[]> lastRow;

    public TransactionDO() {

    }

    public List<String> getData() {
        return data;
    }

    public void setData(List<String> data) {
        this.data = data;
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public void setStopTime(long stopTime) {
        this.stopTime = stopTime;
    }

    public long getPhone() {
        return phone;
    }

    public void setPhone(long phone) {
        this.phone = phone;
    }

    public long getAccount() {
        return account;
    }

    public void setAccount(long account) {
        this.account = account;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Map<String, byte[]> getLastRow() {
        return lastRow;
    }

    public void setLastRow(Map<String, byte[]> lastRow) {
        this.lastRow = lastRow;
    }
}
