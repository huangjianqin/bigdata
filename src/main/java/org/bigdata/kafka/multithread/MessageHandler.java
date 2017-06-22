package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by hjq on 2017/6/19.
 */
public interface MessageHandler<K, V> {
    void setup() throws Exception;
    void handle(ConsumerRecord<K, V> record) throws Exception;
    void cleanup() throws Exception;
}
