package org.bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Created by hjq on 2017/6/22.
 */
public class KafkaProducerThread implements Runnable {
    private KafkaProducer<String, String> producer;
    private long producerId;
    private String topic;

    public KafkaProducerThread(Properties properties, String topic, long producerId) {
        this.producer = new KafkaProducer<String, String>(properties);
        this.topic = topic;
        this.producerId = producerId;
    }

    @Override
    public void run() {
        try{
            int count = 0;
            while(!Thread.currentThread().isInterrupted()){
                producer.send(new ProducerRecord<String, String>(topic, null, "prodcuer-" + producerId + " message" + count));
                try {
                    Thread.sleep(new Random(500).nextInt());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        finally {
            producer.close();
        }
    }
}
