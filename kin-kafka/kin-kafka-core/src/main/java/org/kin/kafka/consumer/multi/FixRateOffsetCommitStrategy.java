package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 默认的commit strategy
 * 根据提交量判断是否允许提交Offset
 * <p>
 * Created by hjq on 2017/6/21.
 */
public class FixRateOffsetCommitStrategy<K, V> implements OffsetCommitStrategy<K, V> {
    private final long rate;
    /** 已提交Offset数 */
    private volatile long counter = 0L;

    public FixRateOffsetCommitStrategy() {
        this(100);
    }

    public FixRateOffsetCommitStrategy(long rate) {
        this.rate = rate;
    }

    @Override
    public void setup(KafkaFetchConfig<K, V> fetchConfig) {

    }

    @Override
    public boolean isToCommit(KafkaMessageHandler<K, V> kafkaMessageHandler, ConsumerRecord<K, V> record) {
        if (++counter % rate == 0) {
            counter = 0L;
            return true;
        }

        return false;
    }

    @Override
    public void shutdown() {

    }
}
