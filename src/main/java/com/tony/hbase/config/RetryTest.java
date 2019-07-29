package com.tony.hbase.config;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.math.RandomUtils;

import java.util.Random;

/**
 * @author tony
 * @describe RetryTet
 * @date 2019-07-29
 */
public class RetryTest {
    public static int RETRY_BACKOFF[] = {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};

    public static  long retry(int pause, int ntries) {
        long normalPause = pause * RETRY_BACKOFF[ntries];
        long jitter = (long) (normalPause * RandomUtils.nextFloat() * 0.01f);
        System.out.println(jitter);
        return normalPause + jitter;
    }

    public static void main(String[] args) {
        for(int i=0;i<50;i++){
            retry(10,6);
        }
    }
}
