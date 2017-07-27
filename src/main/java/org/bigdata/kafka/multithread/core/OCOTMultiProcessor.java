package org.bigdata.kafka.multithread.core;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.bigdata.kafka.multithread.api.*;
import org.bigdata.kafka.multithread.api.AbstractConsumerRebalanceListener;
import org.bigdata.kafka.multithread.monitor.Statistics;
import org.bigdata.kafka.multithread.util.ClassUtil;
import org.bigdata.kafka.multithread.util.ConsumerRecordInfo;
import org.bigdata.kafka.multithread.util.StrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by 健勤 on 2017/7/26.
 * 每个线程一个消费者
 * 主要适用于保证Offset原子消费,尽管是Consumer Rebalance,这个特性多线程不好做,无法保证当前处理的消息
 * 马上发送到broker并让新的Consumer感知到.
 *
 * 保证了consumer rebalance时的exactly once语义
 *
 * 缺点:多个消费者占用资源会更多,单线程处理消费者分配的分区消息,速度会较慢
 */
public class OCOTMultiProcessor<K, V> {
    private static final Logger log = LoggerFactory.getLogger(OCOTMultiProcessor.class);
    private final int consumerNum;
    private final Properties properties;
    private final Set<String> topics;
    private final Class<? extends MessageHandler> messageHandlerClass;
    private final Class<? extends CommitStrategy> commitStrategyClass;
    private final Class<? extends ConsumerRebalanceListener> consumerRebalanceListenerClass;
    private final Class<? extends CallBack> callBackClass;
    private final ThreadPoolExecutor threads;
    private List<OCOTProcessor<K, V>> processors = new ArrayList<>();

    public OCOTMultiProcessor(int consumerNum,
                              Properties properties,
                              Set<String> topics,
                              Class<? extends MessageHandler> messageHandlerClass,
                              Class<? extends CommitStrategy> commitStrategyClass,
                              Class<? extends ConsumerRebalanceListener> consumerRebalanceListenerClass,
                              Class<? extends CallBack> callBackClass) {
        this.consumerNum = consumerNum;
        this.properties = properties;
        this.topics = topics;
        this.messageHandlerClass = messageHandlerClass;
        this.commitStrategyClass = commitStrategyClass;
        this.consumerRebalanceListenerClass = consumerRebalanceListenerClass;
        this.callBackClass = callBackClass;
        this.threads = (ThreadPoolExecutor) Executors.newFixedThreadPool(consumerNum);
    }

    public void start(){
        log.info("start [" + consumerNum + "] message processors...");
        for(int i = 0; i < consumerNum; i++){
            OCOTProcessor<K, V> processor = newProcessor(i);
            processors.add(processor);
            threads.submit(processor);
        }
        log.info("[" + consumerNum + "] message processors started");
    }

    public void close(){
        log.info("[" + consumerNum + "] message processors closing");
        for(OCOTProcessor<K, V> processor: processors){
            processor.close();
        }

        log.info("shutdown threadpool...");
        threads.shutdown();
        log.info("threadpool shutdowned");

        log.info("[" + consumerNum + "] message processors closed");
    }

    private OCOTProcessor<K, V> newProcessor(int processorId){
            return new OCOTProcessor<>(processorId,
                            properties,
                            topics,
                            ClassUtil.instance(messageHandlerClass),
                            ClassUtil.instance(commitStrategyClass),
                            consumerRebalanceListenerClass,
                            callBackClass != null ? ClassUtil.instance(callBackClass) : null);
    }

    public class OCOTProcessor<K, V> implements Runnable {
        private final Logger log = LoggerFactory.getLogger(OCOTProcessor.class);
        private final int processorId;
        private final KafkaConsumer<K, V>  consumer;
        private final MessageHandler<K, V> messageHandler;
        private final CommitStrategy commitStrategy;
        private final AbstractConsumerRebalanceListener consumerRebalanceListener;
        private final CallBack callBack;
        private boolean isStopped = false;
        private Map<TopicPartition, ConsumerRecord> topicPartition2ConsumerRecord = new HashMap<>();

        private OCOTProcessor(int processorId,
                              Properties properties,
                              Set<String> topics,
                              MessageHandler<K, V> messageHandler,
                              CommitStrategy commitStrategy,
                              Class<? extends ConsumerRebalanceListener> consumerRebalanceListenerClass,
                              CallBack callBack) {
            this.processorId = processorId;
            this.consumer = new KafkaConsumer<K, V>(properties);
            this.messageHandler = messageHandler;
            this.commitStrategy = commitStrategy;
            this.callBack = callBack;

            if(topics != null && topics.size() > 0){
                if(consumerRebalanceListenerClass != null){
                    this.consumerRebalanceListener = (AbstractConsumerRebalanceListener) ClassUtil.instance(consumerRebalanceListenerClass, this);
                }
                else {
                    this.consumerRebalanceListener = null;
                }
            }
            else {
                this.consumerRebalanceListener = null;
                throw new IllegalStateException("topics must be setted!!!");
            }
        }

        public void init(){
            log.info("initing message processor-" + processorId + " ...");
            try {
                if(messageHandler != null){
                    messageHandler.setup();
                }

                if(commitStrategy != null){
                    commitStrategy.setup();
                }

                if(consumerRebalanceListenerClass != null){
                   consumerRebalanceListener.setup();
                }

                if(callBack != null){
                    callBack.setup();
                }

                if(topics != null && topics.size() > 0){
                    if(consumerRebalanceListenerClass != null){
                        consumer.subscribe(topics, this.consumerRebalanceListener);
                    }
                    else {
                        consumer.subscribe(topics);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info("message processor-" + processorId + " inited");
        }

        public void doHandle(ConsumerRecordInfo<K, V> consumerRecordInfo){
            try {
                messageHandler.handle(consumerRecordInfo.record());
                consumerRecordInfo.callBack(null);
            } catch (Exception e) {
                try {
                    consumerRecordInfo.callBack(e);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }

        public void commit(ConsumerRecordInfo<K, V> consumerRecordInfo){
            if(commitStrategy.isToCommit(messageHandler, consumerRecordInfo.record())){
                commitSync(getOffsets());
                topicPartition2ConsumerRecord.clear();
            }
        }

        public void commitLatest(){
            if(topicPartition2ConsumerRecord.size() > 0){
                commitSync(getOffsets());
                topicPartition2ConsumerRecord.clear();
            }
        }

        public void close(){
            log.info("message processor-" + processorId + " closing...");
            isStopped = true;
        }

        private void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets){
            log.info("message processor-" + processorId + " commit latest Offsets Sync...");
            consumer.commitSync(offsets);
            log.info("message processor-" + processorId + " consumer offsets [" + StrUtil.topicPartitionOffsetsStr(offsets) + "] committed");
            Statistics.instance().append("offset", StrUtil.topicPartitionOffsetsStr(offsets) + System.lineSeparator());
        }

        private Map<TopicPartition, OffsetAndMetadata> getOffsets(){
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for(Map.Entry<TopicPartition, ConsumerRecord> entry: topicPartition2ConsumerRecord.entrySet()){
                offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset() + 1));
            }
            return offsets;
        }

        public Long position(TopicPartition topicPartition){
            return consumer.position(topicPartition);
        }

        public void seekTo(TopicPartition topicPartition, Long offset){
            consumer.seek(topicPartition, offset);
        }

        @Override
        public void run() {
            init();
            log.info("start message processor-" + processorId);
            try{
                while (!isStopped && !Thread.currentThread().isInterrupted()){
                    ConsumerRecords<K, V> records = consumer.poll(1000);
                    log.debug("message processor-" + processorId + " receive [" + records.count() + "] messages");
                    for(ConsumerRecord<K, V> record: records){
                        ConsumerRecordInfo<K, V> consumerRecordInfo = new ConsumerRecordInfo<K, V>(record, callBack);
                        //存在可能消息积压在此处,调用close后可能会阻塞在这里,因此,两次判断isStopped标识来确保调用close后在进行关闭动作
                        if(isStopped){
                            break;
                        }
                        //记录某分区最新处理的ConsumerRecord
                        if(topicPartition2ConsumerRecord.containsKey(consumerRecordInfo.topicPartition())){
                            if(consumerRecordInfo.offset() > topicPartition2ConsumerRecord.get(consumerRecordInfo.topicPartition()).offset()){
                                topicPartition2ConsumerRecord.put(consumerRecordInfo.topicPartition(), consumerRecordInfo.record());
                            }
                        }
                        else{
                            topicPartition2ConsumerRecord.put(consumerRecordInfo.topicPartition(), consumerRecordInfo.record());
                        }
                        //消息处理
                        doHandle(consumerRecordInfo);
                        //判断是否需要commit offset
                        commit(consumerRecordInfo);
                    }

                }
                log.info("message processor-" + processorId + " message processor-" + processorId + "closed");
            }finally {
                log.info("message processor-" + processorId + " clean up message handler and commit strategy");
                try {
                    if(messageHandler != null){
                        messageHandler.cleanup();
                    }

                    if(commitStrategy != null){
                        commitStrategy.cleanup();
                    }

                    if(consumerRebalanceListenerClass != null){
                        consumerRebalanceListener.cleanup();
                    }

                    if(callBack != null){
                        callBack.cleanup();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets();
                if(offsets != null && offsets.size() > 0){
                    commitSync(offsets);
                }
                log.info("message processor-" + processorId + " consumer closing...");
                consumer.close();
                log.info("message processor-" + processorId + " consumer closed");
            }
            log.info("message processor-" + processorId + " terminated");
        }

        public int getProcessorId() {
            return processorId;
        }
    }
}
