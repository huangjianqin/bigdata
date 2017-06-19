package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


/**
 * Created by 37 on 2017/6/19.
 */
public class ConsumerRecordInfo<K, V> {
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

    public void callBack(Exception e) throws Exception {
        callBack.onComplete(record, e);
    }

    public TopicPartition topicPartition(){
        return new TopicPartition(record.topic(), record.partition());
    }

}
