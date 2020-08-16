package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;


/**
 * 对ConsumerRecord的简单封装,添加回调接口,用户可自定义回调接口,进而在消息处理完成后进行一些额外的操作
 * <p>
 * Created by hjq on 2017/6/19.
 */
public class KafkaMessageWrapper<K, V> {
    /** kafka消息 */
    private ConsumerRecord<K, V> record;

    public KafkaMessageWrapper(ConsumerRecord<K, V> record) {
        this.record = record;
    }

    public ConsumerRecord<K, V> record() {
        return record;
    }

    public long offset() {
        return record.offset();
    }

    public TopicPartition topicPartition() {
        return new TopicPartition(record.topic(), record.partition());
    }

    public long recTime() {
        return record.timestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaMessageWrapper)) {
            return false;
        }

        KafkaMessageWrapper<?, ?> that = (KafkaMessageWrapper<?, ?>) o;

        return !(record != null ? !record.equals(that.record) : that.record != null);

    }

    @Override
    public int hashCode() {
        return record != null ? record.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ConsumerRecordInfo{" +
                "record=" + record +
                '}';
    }
}
