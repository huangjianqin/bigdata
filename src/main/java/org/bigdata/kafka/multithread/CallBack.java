package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by 37 on 2017/6/19.
 */
public interface CallBack {
    /**
     * 处理成功时Exception e为null
     * @param record
     * @param e
     */
    void onComplete(ConsumerRecord record, Exception e) throws Exception;
}
