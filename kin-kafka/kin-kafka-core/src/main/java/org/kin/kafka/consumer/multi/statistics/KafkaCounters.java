package org.kin.kafka.consumer.multi.statistics;

import org.kin.framework.concurrent.ExecutionContext;
import org.kin.framework.concurrent.OrderedEventLoop;
import org.kin.framework.counter.Counters;

/**
 * 系统共用的计数器,用户也可以自定义
 * 单线程修改
 * 多线程访问
 * <p>
 * Created by 健勤 on 2017/6/28.
 */
public class KafkaCounters extends OrderedEventLoop<KafkaCounters> {
    private static final KafkaCounters counters = new KafkaCounters();

    public static KafkaCounters getCounters() {
        return counters;
    }

    public static final String GROUP = "kafka";
    public static final String PRODUCER_COUNTER = "producer-counter";
    public static final String PRODUCER_BYTE_COUNTER = "producer-byte-counter";
    public static final String CONSUMER_COUNTER = "consumer-counter";
    public static final String CONSUMER_BYTE_COUNTER = "consumer-byte-counter";

    //--------------------------------------------------------------------------------------------------------------------------------

    private KafkaCounters() {
        super(null, ExecutionContext.fix(1, "kafka-counters"));
    }

    public void increment(String counter) {
        increment(counter, 1);
    }

    public void increment(String counter, long value) {
        receive(c -> Counters.increment(GROUP, counter, value));
    }

    public long count(String counter) {
        return Counters.counterGroup(GROUP).counter(counter).count();
    }

    public void reset() {
        receive(c -> Counters.reset());
    }
}
