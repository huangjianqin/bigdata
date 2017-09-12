package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.CallBack;
import org.kin.kafka.multithread.api.CommitStrategy;
import org.kin.kafka.multithread.api.MessageHandler;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.config.ConfigValue;
import org.kin.kafka.multithread.utils.ClassUtils;
import org.kin.kafka.multithread.utils.ConsumerRecordInfo;
import org.kin.kafka.multithread.utils.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hjq on 2017/6/19.
 * Fetcher
 * 负责抓取信息的线程
 */
public class MessageFetcher<K, V> extends Thread {
    private static Logger log = LoggerFactory.getLogger(MessageFetcher.class);
    private final KafkaConsumer<K, V> consumer;
    //等待提交的Offset
    //用Map的原因是如果同一时间内队列中有相同的topic分区的offset需要提交，那么map会覆盖原有的
    //使用ConcurrentSkipListMap保证key有序,key为TopicPartitionWithTime,其实现是以加入队列的时间来排序
    private ConcurrentHashMap<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap();
    //线程状态标识
    private boolean isTerminated = false;
    //结束循环fetch操作
    private boolean isStopped = false;
    //重试相关属性
    private boolean enableRetry = false;
    private int maxRetry = 5;
    private int nowRetry = 0;
    //consumer poll timeout
    private long pollTimeout = 1000;
    //定时扫描注册中心并在发现新配置时及时更新运行环境
//    private ConfigFetcher configFetcher;
    private final MessageHandlersManager handlersManager;

    //consumer record callback
    private Class<? extends CallBack> callBackClass;

    public MessageFetcher(Properties properties) {
        super("consumer fetcher thread");
        this.consumer = new KafkaConsumer<K, V>(properties);

        //messagehandler.mode => OPOT/OPMT
        String model = properties.get(AppConfig.MESSAGEHANDLER_MODEL).toString().toUpperCase();
        if (model.equals(ConfigValue.OPOT)){
            this.handlersManager = new OPOTMessageHandlersManager();
        }
        else if(model.equals(ConfigValue.OPMT)){
            this.handlersManager = new OPMTMessageHandlersManager(10, Runtime.getRuntime().availableProcessors() * 2 - 1, 10000 * 10000);//10 * 1000 * 10000
        }
        else if(model.equals(ConfigValue.OPMT2)){
            this.handlersManager = new OPMT2MessageHandlersManager();
        }
        else{
            throw new IllegalStateException(model + " => unknown message handler manager model");
        }
    }

    public void subscribe(Collection<String> topics){
        this.consumer.subscribe(topics, new AbstractMessageHandlersManager.InnerConsumerRebalanceListener<>(this));
    }

    public void registerHandlers(Map<String, Class<? extends MessageHandler>> topic2HandlerClass){
        handlersManager.registerHandlers(topic2HandlerClass);
    }

    public void registerCommitStrategies(Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass){
        handlersManager.registerCommitStrategies(topic2CommitStrategyClass);
    }

    public void registerCallBack(Class<? extends CallBack> callBackClass){
        this.callBackClass = callBackClass;
    }

    public void close(){
        log.info("consumer fetcher thread closing...");
        this.isStopped = true;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public Map<TopicPartition, OffsetAndMetadata> getPendingOffsets() {
        return pendingOffsets;
    }

    public MessageHandlersManager getHandlersManager() {
        return handlersManager;
    }

    @Override
    public void run() {
        long offset = -1;
        log.info("consumer fetcher thread started");
        try{
            //jvm缓存,当消息处理线程还没启动完,或者配置更改时,需先缓存消息,等待线程启动好再处理
            Queue<ConsumerRecord<K, V>> msgCache = new LinkedList<>();

            while(!isStopped && !Thread.currentThread().isInterrupted()){
                //查看有没offset需要提交
                Map<TopicPartition, OffsetAndMetadata> offsets = allPendingOffsets();

                if(offsets != null){
                    //有offset需要提交
                    log.info("consumer commit [" + offsets.size() + "] topic partition offsets");
                    commitOffsetsSync(offsets);
                }

                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                for(TopicPartition topicPartition: records.partitions())
                    for(ConsumerRecord<K, V> record: records.records(topicPartition)){
                        if(record.offset() > offset){
                            offset = record.offset();
                        }
                        //按照某种策略提交线程处理
                        if(!handlersManager.dispatch(new ConsumerRecordInfo(record, callBackClass != null? ClassUtils.instance(callBackClass) : null), pendingOffsets)){
                            log.info("OPOTMessageHandlersManager reconfig...");
                            log.info("message " + record.toString() + " add cache");
                            msgCache.add(record);
                        }
                    }

//                if(!handlersManager.isReConfig()){
//                    Iterator<ConsumerRecord<K, V>> iterator = msgCache.iterator();
//                    log.info("consumer[" + StrUtils.topicPartitionsStr(assignment()) + "] handle cached message");
//                    while(iterator.hasNext()){
//                        ConsumerRecord<K, V> record = iterator.next();
//                        if(handlersManager.dispatch(new ConsumerRecordInfo(record, System.currentTimeMillis()), pendingOffsets)){
//                            //删除该元素
//                            iterator.remove();
//                        }
//                        else{
//                            //可能MessageHandlersManager又发生重新配置了,不处理后面的缓存
//                            log.info("OPOTMessageHandlersManager reconfig when handle cached message[bad]");
//                            break;
//                        }
//                    }
//                }

            }
        }
        finally {
            //等待所有该消费者接受的消息处理完成
            Set<TopicPartition> topicPartitions = this.consumer.assignment();
            handlersManager.consumerCloseNotify();
            //关闭前,提交所有offset
            Map<TopicPartition, OffsetAndMetadata> topicPartition2Offset = allPendingOffsets();
            //同步提交
            commitOffsetsSync(topicPartition2Offset);
            log.info("consumer kafka conusmer closing...");
            this.consumer.close();
            log.info("consumer kafka conusmer closed");
            //有异常抛出,需设置,不然手动调用close就设置好了.
            isStopped = true;
            //标识线程停止
            isTerminated = true;
            log.info("consumer message fetcher closed");
            System.out.println("消费者端接受最大Offset: " + offset);
        }
    }

    public long position(TopicPartition topicPartition){
        return consumer.position(topicPartition);
    }

    public void seek(TopicPartition topicPartition, Long offset){
        consumer.seek(topicPartition, offset);
    }

    public void seekToEnd(Collection<TopicPartition> topicPartitions){
        consumer.seekToEnd(topicPartitions);
    }

    public void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets){
        if(offsets == null || offsets.size() <= 0){
            return;
        }

        log.info("consumer commit offsets Sync...");
        consumer.commitSync(offsets);
//        Statistics.instance().append("offset", StrUtils.topicPartitionOffsetsStr(offsets) + System.lineSeparator());
        log.info("consumer offsets [" + StrUtils.topicPartitionOffsetsStr(offsets) + "] committed");
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets){
        if(offsets == null || offsets.size() <= 0){
            return;
        }

        log.info("consumer commit offsets ASync...");
        consumer.commitAsync(offsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                //Exception e --> The exception thrown during processing of the request, or null if the commit completed successfully
                if (e != null) {
                    log.info("consumer commit offsets " + nowRetry + " times failed!!!");
                    //失败
                    if (enableRetry) {
                        //允许重试,再次重新提交offset
                        if (nowRetry < maxRetry) {
                            log.info("consumer retry commit offsets");
                            commitOffsetsAsync(offsets);
                            nowRetry++;
                        } else {
                            log.error("consumer retry times greater than " + maxRetry + " times(MaxRetry times)");
                            close();
                        }
                    } else {
                        //不允许,直接关闭该Fetcher
                        log.error("consumer disable retry commit offsets");
                        close();
                    }
                } else {
                    //成功,打日志
                    nowRetry = 0;
                    log.info("consumer offsets [" + StrUtils.topicPartitionOffsetsStr(offsets) + "] committed");
                }
            }
        });
    }

    public void commitOffsetsSyncWhenRebalancing(){
        commitOffsetsSync(allPendingOffsets());
    }

    private Map<TopicPartition, OffsetAndMetadata> allPendingOffsets(){
        if(this.pendingOffsets.size() > 0){
            //复制所有需要提交的offset
            synchronized (this.pendingOffsets){
                Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
                for(Map.Entry<TopicPartition, OffsetAndMetadata> entry: this.pendingOffsets.entrySet()){
                    result.put(entry.getKey(), entry.getValue());
                }
                this.pendingOffsets.clear();
                return result;
            }
        }
        return null;
    }


}
