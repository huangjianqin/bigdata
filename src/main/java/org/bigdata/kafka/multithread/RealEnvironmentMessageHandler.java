package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by 健勤 on 2017/7/20.
 */
public class RealEnvironmentMessageHandler extends DefaultMessageHandler{

    @Override
    public void handle(ConsumerRecord<String, String> record) throws Exception {
        Thread.sleep(300);
        super.handle(record);
    }
}
