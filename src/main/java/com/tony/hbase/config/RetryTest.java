package com.tony.hbase.config;

import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.math.RandomUtils;

/**
 * @author tony
 * @describe RetryTet
 * @date 2019-07-29
 */
public class RetryTest {
    public static int[] RETRY_BACKOFF = {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};

    public static long retry(int pause, int ntries) {
        //这里可以看出来,可以优化成到达一定重试次数再执行到下一个
        long normalPause = pause * RETRY_BACKOFF[ntries];
        //这里添加了抖动,防止使键分布更加均匀
        long jitter = (long) (normalPause * RandomUtils.nextFloat() * 0.01f);
        System.out.println(jitter);
        return normalPause + jitter;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 50; i++) {
            retry(10, 6);
        }
    }
}
