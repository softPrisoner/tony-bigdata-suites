package com.tony.hbase.util;

import com.tony.hbase.config.TonyHbaseConf;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author tony
 * @describe HbaseRowKeyUtils
 * @date 2019-07-24
 */
public class HbaseRowKeyUtils {

    public HbaseRowKeyUtils() {
    }

    public static byte[] getAcctRowKey(long account, String channel, long timestamp, long serialNo) throws Exception {
        ByteBuffer keyBuffer = ByteBuffer.allocate(28);
        keyBuffer
                .put(Objects.requireNonNull(TonyHbaseConf.getIdx(account)))
                .put(channel.getBytes()).putLong(account).putLong(timestamp)
                .putLong(serialNo);

        return keyBuffer.array();
    }

    public static byte[] getIdRowKey(String id, String channel, long timestamp, long serialNo) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(38);
        byteBuffer
                .put(TonyHbaseConf.getIdx(id))
                .put(channel.getBytes())
                .put(id.getBytes())
                .putLong(timestamp)
                .putLong(serialNo);

        return byteBuffer.array();
    }

    public static byte[] getPhoneRowKey(long phoneNo, String channel, long timestamp, long serialNo) {
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.put(Objects.requireNonNull(TonyHbaseConf.getIdx(phoneNo)))
                .put(channel.getBytes())
                .putLong(timestamp)
                .putLong(serialNo);

        return buffer.array();
    }

    public static byte[] getTimeRowKey(long serialNo, String channel, long timestamp) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer
                .put(Objects.requireNonNull(TonyHbaseConf.getIdx(serialNo)))
                .put(channel.getBytes()).putLong(timestamp);
        return byteBuffer.array();
    }

    public static byte[] getIdxRowKey(long serialNo) {
        ByteBuffer keyBuffer = ByteBuffer.allocate(10);
        keyBuffer
                .put(Objects.requireNonNull(TonyHbaseConf.getIdx(serialNo)))
                .putLong(serialNo);

        return keyBuffer.array();
    }

    public static byte[] getRegionRowkey(String region, String channel, long timestamp, long serialNo) {
        ByteBuffer keyBuffer = ByteBuffer.allocate(20);
        keyBuffer
                .put(Objects.requireNonNull(TonyHbaseConf.getIdx(serialNo)))
                .put(channel.getBytes())
                .putLong(timestamp)
                .putLong(serialNo);

        return keyBuffer.array();
    }
}
