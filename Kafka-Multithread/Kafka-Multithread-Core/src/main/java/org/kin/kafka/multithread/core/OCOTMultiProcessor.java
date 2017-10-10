package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.*;
import org.kin.kafka.multithread.common.DefaultThreadFactory;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ReConfigable;
import org.kin.kafka.multithread.statistics.Statistics;
import org.kin.kafka.multithread.api.AbstractConsumerRebalanceListener;
import org.kin.kafka.multithread.utils.ClassUtils;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.kin.kafka.multithread.common.ConsumerRecordInfo;
import org.kin.kafka.multithread.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
public class OCOTMultiProcessor<K, V>  implements Application{
    private static final Logger log = LoggerFactory.getLogger(OCOTMultiProcessor.class);
    private int consumerNum;
    private Properties config;
    private Set<String> topics;
    private final Class<? extends MessageHandler> messageHandlerClass;
    private final Class<? extends CommitStrategy> commitStrategyClass;
    private final Class<? extends ConsumerRebalanceListener> consumerRebalanceListenerClass;
    private Class<? extends CallBack> callBackClass;
    /**
     * keepalive改为5s,目的是减少多余线程对系统性能的影响,因为在OCOT模式下,处理线程是固定的
     * 更新配置和Rebalance有可能导致多余线程在线程池
     */
    private ThreadPoolExecutor threads;
    private List<OCOTProcessor<K, V>> processors = new ArrayList<>();

    private AtomicBoolean isReConfig = new AtomicBoolean(false);

    public OCOTMultiProcessor(Properties config) {
        this.consumerNum = Integer.valueOf(config.getProperty(AppConfig.OCOT_CONSUMERNUM));
        this.config = config;
        this.messageHandlerClass = AppConfigUtils.getMessageHandlerClass(config);
        this.commitStrategyClass = AppConfigUtils.getCommitStrategyClass(config);
        this.consumerRebalanceListenerClass = AppConfigUtils.getConsumerRebalanceListenerClass(config);
        updataConfig(config);
        this.threads = new ThreadPoolExecutor(
                2,
                Integer.MAX_VALUE,
                5,
                TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new DefaultThreadFactory(config.getProperty(AppConfig.APPNAME), "OCOTProcessor")
        );
    }

    private void updataConfig(Properties config){
        if(config.getProperty(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).contains("-")){
            throw new IllegalStateException("OCOT doesn't support set messagehandler per topic");
        }
        this.topics = AppConfigUtils.getSubscribeTopic(config);
        this.callBackClass = AppConfigUtils.getCallbackClass(config);
    }

    @Override
    public void start(){
        log.info("start [" + consumerNum + "] message processors...");
        for(int i = 0; i < consumerNum; i++){
            OCOTProcessor<K, V> processor = newProcessor(i);
            processors.add(processor);
            threads.submit(processor);
        }
        log.info("[" + consumerNum + "] message processors started");
    }

    @Override
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

    @Override
    public Properties getConfig() {
        return this.config;
    }

    private OCOTProcessor<K, V> newProcessor(int processorId){
            return new OCOTProcessor<>(processorId,
                            config,
                            topics,
                            ClassUtils.instance(messageHandlerClass),
                            ClassUtils.instance(commitStrategyClass),
                            consumerRebalanceListenerClass,
                            callBackClass != null ? ClassUtils.instance(callBackClass) : null);
    }

    @Override
    public void reConfig(Properties newConfig) {
        log.info("OCOTMultiProcessor reconfiging...");
        //等待上次更新配置完成
        while(!isReConfig.compareAndSet(false, true)){
            log.warn("kafka consumer last reconfig still running!!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        int consumerNum = this.consumerNum;
        if(AppConfigUtils.isConfigItemChange(consumerNum, newConfig, AppConfig.OCOT_CONSUMERNUM)){
            consumerNum = Integer.valueOf(newConfig.getProperty(AppConfig.OCOT_CONSUMERNUM));
            if(consumerNum > 0){
                if(consumerNum > this.consumerNum){
                    log.info("add " + (consumerNum - this.consumerNum) + " consumer processor");
                    //更新正在运行的Processor配置
                    for(int i = 0; i < this.consumerNum; i++){
                        processors.get(i).reConfig(newConfig);
                    }

                    //添加新的消费者线程
                    int nowProcessorId = processors.get(processors.size() - 1).getProcessorId() + 1;
                    for(int i = nowProcessorId;
                        i < nowProcessorId + (consumerNum - this.consumerNum);
                        i++){
                        OCOTProcessor<K, V> processor = newProcessor(i);
                        processors.add(processor);
                        threads.submit(processor);
                    }
                }
                else if(consumerNum < this.consumerNum){
                    log.info("remove " + (this.consumerNum - consumerNum) + " consumer processor");
                    //移除多余的Processor
                    for(int i = 0; i < this.consumerNum - consumerNum; i++){
                        OCOTProcessor processor = processors.remove(0);
                        processor.close();
                    }
                    //更新余下的Processor配置
                    for(OCOTProcessor processor: processors){
                        processor.reConfig(newConfig);
                    }
                }
                log.info("config '" + AppConfig.OCOT_CONSUMERNUM + "' change from '" + this.consumerNum + "' to '" + consumerNum + "'");
                this.consumerNum = consumerNum;
            }
            else {
                throw new IllegalStateException("config '" + AppConfig.OCOT_CONSUMERNUM + "' state wrong");
            }
        }

        //更新本地配置
        this.config = newConfig;

        isReConfig.compareAndSet(true,false);
        log.info("OCOTMultiProcessor reconfiged");
    }

    public class OCOTProcessor<K, V> implements Runnable, ReConfigable {
        private final Logger log = LoggerFactory.getLogger(OCOTProcessor.class);
        private final int processorId;
        private final KafkaConsumer<K, V>  consumer;
        private long pollTimeout;
        private final MessageHandler<K, V> messageHandler;
        private final CommitStrategy commitStrategy;
        private final AbstractConsumerRebalanceListener consumerRebalanceListener;
        private final CallBack callBack;
        private boolean isStopped = false;
        private Map<TopicPartition, ConsumerRecord> topicPartition2ConsumerRecord = new HashMap<>();

        //kafka consumer订阅topic partition
        private List<TopicPartition> subscribed;

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
                    this.consumerRebalanceListener = (AbstractConsumerRebalanceListener) ClassUtils.instance(consumerRebalanceListenerClass, this);
                }
                else {
                    this.consumerRebalanceListener = null;
                }
            }
            else {
                this.consumerRebalanceListener = null;
                throw new IllegalStateException("topics must not be null!!!");
            }
        }

        public void init(){
            log.info("initing message processor-" + processorId + " ...");
            try {
                if(messageHandler != null){
                    messageHandler.setup(config);
                }

                if(commitStrategy != null){
                    commitStrategy.setup(config);
                }

                if(consumerRebalanceListenerClass != null){
                   consumerRebalanceListener.setup();
                }

                if(callBack != null){
                    callBack.setup(config, null);
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
                consumerRecordInfo.callBack(messageHandler, commitStrategy,null);
            } catch (Exception e) {
                try {
                    consumerRecordInfo.callBack(messageHandler, commitStrategy, e);
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
            log.info("message processor-" + processorId + " consumer offsets [" + TPStrUtils.topicPartitionOffsetsStr(offsets) + "] committed");
            Statistics.instance().append("offset", TPStrUtils.topicPartitionOffsetsStr(offsets) + System.lineSeparator());
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
                    //重新导入配置中....
                    //会停止接受消息(因为consumer与broker存在session timout,该过程要尽量快)
                    //message handler会停止处理消息
                    if(isReConfig.get()){
                        log.info("kafka consumer pause receive all records");
                        //提交队列中待提交的Offsets
                        Map<TopicPartition, OffsetAndMetadata> topicPartition2Offset = getOffsets();
                        commitSync(topicPartition2Offset);
                        //停止消费消息
                        consumer.pause(subscribed);
                        consumer.poll(0);
                    }

                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    //缓存订阅的topic-partition
                    if(subscribed == null){
                        subscribed = new ArrayList<>(consumer.assignment());
                    }
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
            }
            catch (Exception e){
                e.printStackTrace();
            }
            finally {
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

        @Override
        public void reConfig(Properties newConfig) {
            if(AppConfigUtils.isConfigItemChange(config, newConfig, AppConfig.MESSAGEFETCHER_POLL_TIMEOUT)){
                pollTimeout = Long.valueOf(newConfig.get(AppConfig.MESSAGEFETCHER_POLL_TIMEOUT).toString());
            }

            //恢复接受已订阅分区的消息
            consumer.resume(subscribed);
        }
    }
}
