package org.bigdata.kafka.multithread;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 健勤 on 2017/6/28.
 */
public class Counters {
    private static Counters counters = new Counters();
    private ConcurrentHashMap<String, AtomicLong> name2Counter = new ConcurrentHashMap<>();

    public static Counters getCounters() {
        return counters;
    }

    private Counters() {
        name2Counter.put("producer-counter", new AtomicLong(0));
        name2Counter.put("consumer-counter", new AtomicLong(0));
    }

    public void add(String name){
        AtomicLong counter = name2Counter.get(name);

        if(counter != null){
            counter.addAndGet(1);
        }
    }

    public Long get(String name){
        AtomicLong counter = name2Counter.get(name);

        if(counter != null){
            return counter.get();
        }

        return -1L;
    }
}
