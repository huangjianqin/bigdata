package org.bigdata.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.bigdata.kafka.multithread.Counters;
import org.bigdata.kafka.multithread.PropertiesWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 37 on 2017/6/22.
 */
public class KafkaProducerThreadGroup {
    public static void main(String[] args) throws InterruptedException {
        long startTime = 0;
        long endTime = 0;

        Properties config = PropertiesWrapper.create()
                .set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092")
                .set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .properties();

        int threadSize = 1;
        int runTime = 1 * 2 * 1000;

        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
        List<KafkaProducerThread> threads = new ArrayList<>();
        for(int i = 1; i <= threadSize; i++){
            startTime = System.currentTimeMillis();
            KafkaProducerThread thread = new KafkaProducerThread(config, "multi-msg", i);
            threads.add(thread);
            executorService.submit(thread);
        }

        Thread.sleep(runTime);
        endTime = System.currentTimeMillis();
        for(KafkaProducerThread thread: threads){
            thread.close();
        }
        Thread.sleep(2000);
        executorService.shutdown();

        long sum = Counters.getCounters().get("producer-counter");
        System.out.println("总发送: " + sum);
        System.out.println("总发送率: " + 1.0 * sum / (endTime - startTime));
    }
}
