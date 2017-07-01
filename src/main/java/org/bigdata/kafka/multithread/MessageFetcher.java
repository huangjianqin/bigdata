package org.bigdata.kafka.multithread;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

/**
 * Created by hjq on 2017/6/19.
 */
public class MessageFetcher<K, V> implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MessageFetcher.class);
    private KafkaConsumer<K, V> consumer;
    //等待提交的Offset
    //用Map的原因是如果同一时间内队列中有相同的topic分区的offset需要提交，那么map会覆盖原有的
    //使用ConcurrentSkipListMap保证key有序,key为TopicPartitionWithTime,其实现是以加入队列的时间来排序
    private ConcurrentHashMap<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap();
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
    private MessageHandlersManager handlersManager;

    public MessageFetcher(Properties properties) {
        this.consumer = new KafkaConsumer<K, V>(properties);
        this.handlersManager = new MessageHandlersManager();
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener){
        this.consumer.subscribe(topics, listener);
    }

    public void subscribe(Collection<String> topics){
        this.consumer.subscribe(topics);
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener){
        this.consumer.subscribe(pattern, listener);
    }

    public void registerHandlers(Map<String, MessageHandler> topic2Handler){
        handlersManager.registerHandlers(topic2Handler);
    }

    public void registerCommitStrategies(Map<TopicPartition, CommitStrategy> topic2CommitStrategy){
        handlersManager.registerCommitStrategies(topic2CommitStrategy);
    }

    public void close(){
        log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] fetcher thread closing...");
        this.isStopped = true;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public Set<TopicPartition> assignment(){
        return new HashSet<>();
    }

    public Map<TopicPartitionWithTime, OffsetAndMetadata> getPendingOffsets() {
        return pendingOffsets;
    }

    @Override
    public void run() {
        log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] fetcher thread started");
        try{
            //jvm缓存,当消息处理线程还没启动完,或者配置更改时,需先缓存消息,等待线程启动好再处理
            Queue<ConsumerRecord<K, V>> msgCache = new LinkedList<>();

            while(!isStopped && !Thread.currentThread().isInterrupted()){
                //查看有没offset需要提交
                Map<TopicPartition, OffsetAndMetadata> offsets = allPendingOffsets();

                if(offsets != null){
                    //有offset需要提交
                    log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] commit [" + offsets.size() + "] topic partition offsets");
                    commitOffsetsSync(offsets);
                }

                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                for(TopicPartition topicPartition: records.partitions())
                    for(ConsumerRecord<K, V> record: records.records(topicPartition)){
                        //按照某种策略提交线程处理
                        if(!handlersManager.dispatch(new ConsumerRecordInfo(record, System.currentTimeMillis()), pendingOffsets)){
                            log.info("MessageHandlersManager reconfig...");
                            log.info("message " + record.toString() + " add cache");
                            msgCache.add(record);
                        }
                    }

//                if(!handlersManager.isReConfig()){
//                    Iterator<ConsumerRecord<K, V>> iterator = msgCache.iterator();
//                    log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] handle cached message");
//                    while(iterator.hasNext()){
//                        ConsumerRecord<K, V> record = iterator.next();
//                        if(handlersManager.dispatch(new ConsumerRecordInfo(record, System.currentTimeMillis()), pendingOffsets)){
//                            //删除该元素
//                            iterator.remove();
//                        }
//                        else{
//                            //可能MessageHandlersManager又发生重新配置了,不处理后面的缓存
//                            log.info("MessageHandlersManager reconfig when handle cached message[bad]");
//                            break;
//                        }
//                    }
//                }

            }
        }
        finally {
            //等待所有该消费者接受的消息处理完成
            Set<TopicPartition> topicPartitions = this.consumer.assignment();
            handlersManager.consumerCloseNotify(topicPartitions);
            //关闭前,提交所有offset
            Map<TopicPartition, OffsetAndMetadata> topicPartition2Offset = allPendingOffsets();
            log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] commit offsets Sync...");
            //同步提交
            consumer.commitSync(topicPartition2Offset);
            log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] commit offsets [" + StrUtil.topicPartitionOffsetsStr(topicPartition2Offset) + "]");
            log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] kafka conusmer closing...");
            this.consumer.close();
            log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] kafka conusmer closed");
            //有异常抛出,需设置,不然手动调用close就设置好了.
            isStopped = true;
            //标识线程停止
            isTerminated = true;
            log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] message fetcher closed");
        }
    }

    private void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets){
        log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] commit offsets Sync...");
        consumer.commitSync(offsets);
        log.info("consumer[" + StrUtil.topicPartitionsStr(MessageFetcher.this.assignment()) + "] offsets [" + StrUtil.topicPartitionOffsetsStr(offsets) + "] committed");
    }

    private void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets){
        log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] commit offsets ASync...");
        consumer.commitAsync(offsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                //Exception e --> The exception thrown during processing of the request, or null if the commit completed successfully
                if(e !=null){
                    log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] commit offsets " + nowRetry + " times failed!!!");
                    //失败
                    if(enableRetry){
                        //允许重试,再次重新提交offset
                        if(nowRetry < maxRetry){
                            log.info("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] retry commit offsets");
                            commitOffsetsAsync(offsets);
                            nowRetry ++;
                        }
                        else{
                            log.error("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] retry times greater than " + maxRetry + " times(MaxRetry times)");
                            close();
                        }
                    }
                    else{
                        //不允许,直接关闭该Fetcher
                        log.error("consumer[" + StrUtil.topicPartitionsStr(assignment()) + "] disable retry commit offsets");
                        close();
                    }
                }
                else{
                    //成功,打日志
                    nowRetry = 0;
                    log.info("consumer[" + StrUtil.topicPartitionsStr(MessageFetcher.this.assignment()) + "] offsets [" + StrUtil.topicPartitionOffsetsStr(offsets) + "] committed");
                }
            }
        });
    }

    private Map<TopicPartition, OffsetAndMetadata> allPendingOffsets(){
        if(this.pendingOffsets.size() > 0){
            //复制所有需要提交的offset
            synchronized (this.pendingOffsets){
                Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
                for(Map.Entry<TopicPartitionWithTime, OffsetAndMetadata> entry: this.pendingOffsets.entrySet()){
                    result.put(entry.getKey().topicPartition(), entry.getValue());
                }
                this.pendingOffsets.clear();
                return result;
            }
        }
        return null;
    }

    public abstract class InnerRebalanceListener implements ConsumerRebalanceListener{
        protected KafkaConsumer<K, V> consumer = MessageFetcher.this.consumer;
    }

    public class InMemoryRebalanceListsener extends InnerRebalanceListener{
        private Map<TopicPartition, Long> topicPartition2Offset = new HashMap<>();

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            log.info("consumer origin assignment: " + StrUtil.topicPartitionsStr(collection));
            log.info("consumer rebalancing...");
            //保存在jvm内存中
            for(TopicPartition topicPartition: collection){
                topicPartition2Offset.put(topicPartition, consumer.position(topicPartition));
            }

            //还需清理已经交给handler线程
            handlersManager.consumerRebalanceNotify();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            for(TopicPartition topicPartition: collection){
                consumer.seek(topicPartition, topicPartition2Offset.get(topicPartition));
            }
            log.info("consumer reassigned");
            log.info("consumer new assignment: " + StrUtil.topicPartitionsStr(collection));
            //清理offset缓存
            topicPartition2Offset.clear();
        }
    }
}
