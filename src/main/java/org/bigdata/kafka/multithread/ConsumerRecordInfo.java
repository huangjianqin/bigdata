package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


/**
 * Created by hjq on 2017/6/19.
 */
public class ConsumerRecordInfo<K, V>{
    private ConsumerRecord<K, V> record;
    private CallBack callBack;

    public ConsumerRecordInfo(ConsumerRecord<K, V> record) {
        this.record = record;
    }

    public ConsumerRecordInfo(ConsumerRecord<K, V> record, CallBack callBack) {
        this.record = record;
        this.callBack = callBack;
    }

    public ConsumerRecord<K, V> record() {
        return record;
    }

    public long offset(){
        return record.offset();
    }

    public void callBack(Exception e) throws Exception {
        if(callBack != null){
            callBack.onComplete(record, e);
        }
    }

    public TopicPartition topicPartition(){
        return new TopicPartition(record.topic(), record.partition());
    }

    public long recTime() {
        return record.timestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerRecordInfo)) return false;

        ConsumerRecordInfo<?, ?> that = (ConsumerRecordInfo<?, ?>) o;

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
