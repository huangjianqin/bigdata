package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by hjq on 2017/6/19.
 */
public interface DispatchStrategy {
    int dispatch(ConsumerRecord record);
}
