package com.tony.hbase.geode;

import com.esotericsoftware.kryo.util.ObjectMap;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;

import java.util.Map;

/**
 * @author tony
 * @describe GeodeTest
 * @date 2019-08-16
 */
public class GeodeTest {
    public static void main(String[] args) {
        ClientCache cache = new ClientCacheFactory()
                .addPoolLocator("localhost", 10334).create();
        Region<String, String> region = cache.<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("test");
        region.put("1", "hello");
        region.put("2", "world");
        for (Map.Entry<String, String> entry : region.entrySet()) {
            System.out.format("key=%s,value=%s \n", entry.getKey(), entry.getValue());
        }
        cache.close();
    }
}
