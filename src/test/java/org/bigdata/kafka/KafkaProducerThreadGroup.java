package org.bigdata.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.bigdata.kafka.multithread.PropertiesWrapper;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 37 on 2017/6/22.
 */
public class KafkaProducerThreadGroup {
    public static void main(String[] args) throws InterruptedException {
        Properties config = PropertiesWrapper.create()
                .set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092")
                .set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .properties();

        int threadSize = 2;
        int runTime = 1 * 30 * 1000;

        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
        for(int i = 1; i <= threadSize; i++){
            executorService.submit(new KafkaProducerThread(config, "multi-msg", i));
        }

        Thread.sleep(runTime);
        executorService.shutdownNow();
    }
}
