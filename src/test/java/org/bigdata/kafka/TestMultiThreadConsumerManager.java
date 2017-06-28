package org.bigdata.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.bigdata.kafka.api.MultiThreadConsumerManager;
import org.bigdata.kafka.multithread.Counters;
import org.bigdata.kafka.multithread.PropertiesWrapper;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by hjq on 2017/6/22.
 */
public class TestMultiThreadConsumerManager {
    public static void main(String[] args) throws InterruptedException {
        long startTime = 0;
        long endTime = 0;

        Properties config = PropertiesWrapper.create()
                .set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092")
                .set(ConsumerConfig.GROUP_ID_CONFIG, "multi-handle")
                .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .properties();
        Set<String> topic = new HashSet<>();
        topic.add("multi-msg");

        //1.一个consumer
        MultiThreadConsumerManager.instance().<String, String>registerConsumer("test", config, topic, null, null);
        startTime = System.currentTimeMillis();

        long runTime = 60 * 1000;
        Thread.sleep(runTime);
        endTime = System.currentTimeMillis();

        MultiThreadConsumerManager.instance().stopConsumerSync("test");

        long sum = Counters.getCounters().get("consumer-counter");
        System.out.println("总消费:" + sum);
        System.out.println("总消费率: " + 1.0 * sum / (endTime - startTime));
    }
}
