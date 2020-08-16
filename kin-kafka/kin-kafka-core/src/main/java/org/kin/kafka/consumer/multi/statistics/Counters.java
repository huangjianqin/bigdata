package org.kin.kafka.consumer.multi.statistics;

import org.kin.framework.concurrent.ExecutionContext;
import org.kin.framework.concurrent.actor.PinnedThreadSafeHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 系统共用的计数器,用户也可以自定义
 * 单线程修改
 * 多线程访问
 * <p>
 * Created by 健勤 on 2017/6/28.
 */
public class Counters extends PinnedThreadSafeHandler<Counters> {
    private static final Counters counters = new Counters();

    public static Counters getCounters() {
        return counters;
    }

    public static String PRODUCER_COUNTER = "producer-counter";
    public static String PRODUCER_BYTE_COUNTER = "producer-byte-counter";
    public static String CONSUMER_COUNTER = "consumer-counter";
    public static String CONSUMER_BYTE_COUNTER = "consumer-byte-counter";

    //--------------------------------------------------------------------------------------------------------------------------------

    /** key -> name, value -> value */
    private ConcurrentMap<String, Long> name2Counter = new ConcurrentHashMap<>();

    private Counters() {
        super(ExecutionContext.fix(1, "kafka-counters"));
        name2Counter.put(PRODUCER_COUNTER, 0L);
        name2Counter.put(PRODUCER_BYTE_COUNTER, 0L);
        name2Counter.put(CONSUMER_COUNTER, 0L);
        name2Counter.put(CONSUMER_BYTE_COUNTER, 0L);
    }

    public void increment(String name) {
        add(name, 1);
    }

    public void add(String name, long value) {
        handle(c -> {
            long old = get(name);
            name2Counter.put(name, old + value);
        });
    }

    public long get(String name) {
        return name2Counter.getOrDefault(name, 0L);
    }

    public void reset() {
        handle(c -> {
            for (String counterKey : name2Counter.keySet()) {
                name2Counter.put(counterKey, 0L);
            }
        });
    }
}
