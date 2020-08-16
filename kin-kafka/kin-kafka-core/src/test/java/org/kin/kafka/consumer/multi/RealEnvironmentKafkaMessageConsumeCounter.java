package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by 健勤 on 2017/7/20.
 * 模拟真实生产环境的message handler
 */
public class RealEnvironmentKafkaMessageConsumeCounter extends KafkaMessageConsumeCounter<String, String> {

    @Override
    public void handle(ConsumerRecord<String, String> record) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {

        }
        super.handle(record);
    }
}
