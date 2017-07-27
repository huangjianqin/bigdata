package org.bigdata.kafka.multithread.monitor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by 健勤 on 2017/7/26.
 */
public class Statistics {
    private static ConcurrentMap<String, StringBuilder> cache = new ConcurrentHashMap<>();
    private static final Statistics statistics = new Statistics();
    public static Statistics instance(){
        return statistics;
    }
    private Statistics() {
        cache.put("offset", new StringBuilder());
    }

    public String get(String key){
        return cache.get(key) != null? cache.get(key).toString() : null;
    }

    public void append(String key, String value){
        cache.get(key).append(value);
    }
}
