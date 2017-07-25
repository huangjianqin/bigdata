package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.bigdata.kafka.api.Counters;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hjq on 2017/6/22.
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
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);

        int threadSize = 2;
        int runTime = 10 * 60 * 1000;

        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
        List<KafkaProducerThread> threads = new ArrayList<>();
        startTime = System.currentTimeMillis();
        for(int i = 1; i <= threadSize; i++){
            KafkaProducerThread thread = new KafkaProducerThread(producer, "multi-msg", i);
            threads.add(thread);
            executorService.submit(thread);
        }

        Thread.sleep(runTime);
        endTime = System.currentTimeMillis();
        for(KafkaProducerThread thread: threads){
            thread.close();
        }
        Thread.sleep(2000);
        producer.close();
        System.out.println("kafka producer stop!");
        executorService.shutdown();

        long sum = Counters.getCounters().get("producer-counter");
        long sum1 = Counters.getCounters().get("producer-byte-counter");
        System.out.println("总发送: " + sum + "条");
        System.out.println("总发送: " + sum1 + "字节");
        System.out.println("总发送率: " + 1.0 * sum / (endTime - startTime) + "条/ms");
        System.out.println("总发送率: " + 1.0 * sum1 / (endTime - startTime) + "B/ms");
        System.out.println("最终Offset: " + KafkaProducerThread.offset);
    }
}
