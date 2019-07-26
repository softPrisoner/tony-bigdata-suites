package com.tony.hbase.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * @author tony
 * @describe HbaseInsertServicePool
 * @date 2019-07-26
 */
public class HbaseInsertServicePool {
    private static final LinkedList<HbaseInsertService> POOL = new LinkedList<>();
    private static int core_size = 3;
    private static int max_size = 5;
    private static int current_size = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseInsertServicePool.class);

    public HbaseInsertServicePool(int coreSize, int maxSize) {
        core_size = coreSize;
        max_size = maxSize;
        createBasicPool();
    }

    public void createBasicPool() {
        if (POOL.size() == 0) {
            for (int i = 0; i < core_size; ++i) {
                HbaseInsertService insertService = new HbaseInsertServiceImpl();
                ++current_size;
                POOL.add(insertService);
            }
        }
    }

    private HbaseInsertService createService() {
        ++core_size;
        POOL.add(new HbaseInsertServiceImpl());
        return POOL.removeFirst();
    }

    public HbaseInsertService getService() {
        if (core_size <= 0) {
            createBasicPool();
        }
        if (POOL.size() > 0) {
            return POOL.removeFirst();
        } else if (core_size < max_size) {
            ++current_size;
            return createService();
        } else {
            try {
                LOGGER.warn(" Idle thread is not enough.Waiting.....");
                Thread.sleep(10L);
                //注意内存泄露
                return getService();
            } catch (InterruptedException e) {
                LOGGER.error("thread size touches the max size");
            }
        }
        return null;
    }

    public static void closeService(HbaseInsertService service) {
        --core_size;
        if (POOL.size() < core_size) {
            POOL.add(service);
        } else {
            service.closeTable();
        }
    }


}
