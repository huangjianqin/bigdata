package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by hjq on 2017/6/21.
 */
public class DefaultMessageHandler implements MessageHandler<String, String>{

    @Override
    public void setup() throws Exception {

    }

    @Override
    public void handle(ConsumerRecord<String, String> record) throws Exception {
        Counters.getCounters().add("consumer-byte-counter", record.value().getBytes().length);
    }

    @Override
    public void cleanup() throws Exception {

    }
}
