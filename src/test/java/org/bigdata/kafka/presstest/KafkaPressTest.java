package org.bigdata.kafka.presstest;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 健勤 on 2017/5/22.
 */
public class KafkaPressTest {
    private static AtomicLong pCounter = new AtomicLong(0L);
    private static AtomicLong cCounter = new AtomicLong(0L);
    private Properties pConfig;
    private Properties cConfig;

    //producer-topic关系
    private Map<String, Integer> producerTopics = new HashMap<>();
    //consumer-topic关系
    private Map<String, Integer> consumerTpoics = new HashMap<>();
    //默认消息大小1K
    private int msgSize = 1024;
    //设置收集数据的描述
    private int seconds = 60;
    //关闭producer开关
    private boolean isFinish = false;

    public KafkaPressTest() {
        this.pConfig = new Properties();
        setP(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
        setP(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        setP(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.pConfig = new Properties();
        setP(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
        setP(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        setP(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    }

    public KafkaPressTest(Properties pConfig, Properties cConfig) {
        this.pConfig = pConfig;
        this.cConfig = cConfig;
    }

    public void start() throws InterruptedException {
        ExecutorService pExecutor = Executors.newFixedThreadPool(getThreadsNum(getProducerTopics()));
        ExecutorService cExecutor = Executors.newFixedThreadPool(getThreadsNum(getConsumerTpoics()));

        for(Map.Entry<String, Integer> entry: getProducerTopics().entrySet()){
            for(int i = 1; i <= entry.getValue(); i++){
                pExecutor.submit(new MsgProducer(i, entry.getKey()));
            }
        }

        for(Map.Entry<String, Integer> entry: getConsumerTpoics().entrySet()){
            for(int i = 1; i <= entry.getValue(); i++){
                pExecutor.submit(new MsgConsumer(entry.getKey() + "-consumer-group", Collections.singletonList(entry.getKey())));
            }
        }

        StringBuilder pResult = new StringBuilder();
        StringBuilder cResult = new StringBuilder();
        int now = 0;
        while(now < seconds){
            //统计每秒发送及消费的消息数
            pResult.append(pCounter.getAndSet(0) + System.lineSeparator());
            cResult.append(cCounter.getAndSet(0) + System.lineSeparator());
            now ++;
            Thread.sleep(1000);
        }

        isFinish = true;
        pExecutor.shutdown();
        cExecutor.shutdown();
        System.out.println("测试结束");
        System.out.println("-------------------------------------------------------------------------------------------------------");
        System.out.println(pResult.toString());
        System.out.println("-------------------------------------------------------------------------------------------------------");
        System.out.printf(cResult.toString());
    }

    public int getThreadsNum(Map<String, Integer> map){
        int sum = 0;
        for(Integer value: map.values()){
            sum += value;
        }
        return sum;
    }

    public static AtomicLong getpCounter() {
        return pCounter;
    }

    public static void setpCounter(AtomicLong pCounter) {
        KafkaPressTest.pCounter = pCounter;
    }

    public static AtomicLong getcCounter() {
        return cCounter;
    }

    public static void setcCounter(AtomicLong cCounter) {
        KafkaPressTest.cCounter = cCounter;
    }

    public Properties getpConfig() {
        return pConfig;
    }

    public void setpConfig(Properties pConfig) {
        this.pConfig = pConfig;
    }

    public Properties getcConfig() {
        return cConfig;
    }

    public void setcConfig(Properties cConfig) {
        this.cConfig = cConfig;
    }

    public void setP(String key, String value){
        this.pConfig.setProperty(key, value);
    }

    public String getP(String key){
        return this.pConfig.getProperty(key);
    }

    public void setC(String key, String value){
        this.cConfig.setProperty(key, value);
    }

    public String getC(String key){
        return this.cConfig.getProperty(key);
    }

    public void addProducerTopic(String topic, int num){
        getProducerTopics().put(topic, num);
    }

    public void addConsumerTopic(String topic, int num){
        getConsumerTpoics().put(topic, num);
    }

    public Map<String, Integer> getProducerTopics() {
        return producerTopics;
    }

    public void setProducerTopics(Map<String, Integer> producerTopics) {
        this.producerTopics = producerTopics;
    }

    public Map<String, Integer> getConsumerTpoics() {
        return consumerTpoics;
    }

    public void setConsumerTpoics(Map<String, Integer> consumerTpoics) {
        this.consumerTpoics = consumerTpoics;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public int getSeconds() {
        return seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }

    class MsgProducer implements Runnable{
        private static final String head = "kafka-prodcuer-";
        private int id;
        private String topic;

        public MsgProducer(int id, String topic) {
            this.id = id;
            this.topic = topic;
        }

        @Override
        public void run() {
            KafkaProducer<String, Byte[]> producer = new KafkaProducer<String, Byte[]>(getpConfig());
            Random random = new Random();

            try{
                int msgCounter = 0;
                while(!isFinish){
                    byte[] bytes = new byte[msgSize];
                    random.nextBytes(bytes);

                    Byte[] msg = new Byte[msgSize];
                    for(int i = 0; i < bytes.length; i++){
                        msg[i] = bytes[i];
                    }
                    msgCounter ++;
                    producer.send(new ProducerRecord<String, Byte[]>(topic, head + id + "-" + msgCounter, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            pCounter.getAndIncrement();
                        }
                    });
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }
        }
    }

    class MsgConsumer implements Runnable{
        private static final String head = "kafka-consumer-";
        private String group;
        private List<String> topics;

        public MsgConsumer(String group, List<String> topics) {
            this.group = group;
            this.topics = topics;
        }

        @Override
        public void run() {
            Properties now = (Properties) cConfig.clone();
            now.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);

            KafkaConsumer<String, Byte[]> consumer = new KafkaConsumer<String, Byte[]>(now);
            consumer.subscribe(topics);

            try{
                while(!isFinish){
                    ConsumerRecords<String, Byte[]> records = consumer.poll(100);
                    for(TopicPartition topicPartition: records.partitions()){
                        List<ConsumerRecord<String, Byte[]>> partitionRecords = records.records(topicPartition);
                        for(ConsumerRecord<String, Byte[]> record: partitionRecords){
                            Thread.sleep(50);
                        }
                        long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)), new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                cCounter.getAndIncrement();
                            }
                        });
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
//        Properties producerConfig = new Properties();
//        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
//        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        Properties consumerConfig = new Properties();
//        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
//        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "user-ratings");
//
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig);
//
//        try{
////            int msgNum = 10;
////            for(int i = 0; i < msgNum; i++){
////                producer.send(new ProducerRecord<String, String>("ratings", System.currentTimeMillis() + "", "1,10005,3"));
////            }
////
//            consumer.subscribe(Collections.singletonList("ratings"));
//            consumer.poll(0);
//            consumer.seekToBeginning(Collections.singletonList(new TopicPartition("ratings", 0)));
//            while(true){
//                ConsumerRecords<String, String> records = consumer.poll(1000);
//                for(ConsumerRecord record: records){
//                    System.out.println(record.key() + " -> " + record.value() + "{" + record.partition() + ", " + record.offset() + "}");
//                }
//            }
//
//        }finally {
//            producer.close();
//            consumer.close();
//        }

    }
}
