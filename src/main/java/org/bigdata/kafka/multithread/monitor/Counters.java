package org.bigdata.kafka.multithread.monitor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 健勤 on 2017/6/28.
 * 系统共用的计数器,用户也可以自定义
 */
public class Counters {
    private static ConcurrentMap<String, AtomicLong> name2Counter = new ConcurrentHashMap<>();
    private static final Counters counters = new Counters();

    public static Counters getCounters() {
        return counters;
    }

    private Counters() {
        name2Counter.put("producer-counter", new AtomicLong(0));
        name2Counter.put("producer-byte-counter", new AtomicLong(0));
        name2Counter.put("consumer-counter", new AtomicLong(0));
        name2Counter.put("consumer-byte-counter", new AtomicLong(0));
    }

    public synchronized void add(String name){
        AtomicLong counter = name2Counter.get(name);

        if(counter != null){
            counter.incrementAndGet();
        }
        else{
            name2Counter.put(name, new AtomicLong(1));
        }
    }

    public synchronized void add(String name, Long value){
        AtomicLong counter = name2Counter.get(name);

        if(counter != null){
            counter.addAndGet(value);
        }
        else{
            name2Counter.put(name, new AtomicLong(value));
        }
    }

    public synchronized void add(String name, Integer value){
        AtomicLong counter = name2Counter.get(name);

        if(counter != null){
            counter.addAndGet(value);
        }
        else{
            name2Counter.put(name, new AtomicLong(value));
        }
    }

    public Long get(String name){
        AtomicLong counter = name2Counter.get(name);

        if(counter != null){
            return counter.get();
        }

        return -1L;
    }

    public synchronized void reset(){
        for(AtomicLong counter: name2Counter.values()){
            counter.set(0);
        }
    }
}
