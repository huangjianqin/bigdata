package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by hjq on 2017/6/21.
 * 仅仅是打印,并睡眠2s
 */
public class DefaultMessageHandler implements MessageHandler<String, String>{

    @Override
    public void setup() throws Exception {

    }

    @Override
    public void handle(ConsumerRecord<String, String> record) throws Exception {
        System.out.println(record.key() + " ---> " + record.value());
        Thread.sleep(2 * 1000);
    }

    @Override
    public void cleanup() throws Exception {

    }
}
