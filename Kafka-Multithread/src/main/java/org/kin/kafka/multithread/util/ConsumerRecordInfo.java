package org.kin.kafka.multithread.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.CallBack;


/**
 * Created by hjq on 2017/6/19.
 * 对ConsumerRecord的简单封装,添加回调接口,用户可自定义回调接口,进而在消息处理完成后进行一些额外的操作
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
            callBack.cleanup();
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
