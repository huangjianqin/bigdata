package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kin.kafka.consumer.multi.statistics.KafkaCounters;

/**
 * 默认的Message handler
 * 仅仅添加相应的计数器
 * <p>
 * Created by hjq on 2017/6/21.
 */
public class KafkaMessageConsumeCounter<K, V> implements KafkaMessageHandler<K, V> {
    @Override
    public void setup(KafkaFetchConfig<K, V> fetchConfig) {

    }

    @Override
    public void handle(ConsumerRecord<K, V> record) {
        KafkaCounters.getCounters().increment(KafkaCounters.CONSUMER_COUNTER);
        KafkaCounters.getCounters().increment(KafkaCounters.CONSUMER_BYTE_COUNTER, record.value().toString().getBytes().length);
    }

    @Override
    public void shutdown() {

    }
}
