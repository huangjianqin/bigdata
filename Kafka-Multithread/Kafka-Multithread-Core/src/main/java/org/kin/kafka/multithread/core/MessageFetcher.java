package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.CallBack;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.statistics.Statistics;
import org.kin.kafka.multithread.utils.ClassUtils;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.kin.kafka.multithread.common.ConsumerRecordInfo;
import org.kin.kafka.multithread.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hjq on 2017/6/19.
 * Fetcher
 * 负责抓取信息的线程
 */
public class MessageFetcher<K, V> extends Thread implements Application {
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
    //重试commit offset的相关属性
    private boolean enableRetry = false;
    private int maxRetry;
    private int nowRetry = 0;
    //consumer poll timeout
    private long pollTimeout;

    private final MessageHandlersManager handlersManager;
    //consumer record callback
    private Class<? extends CallBack> callBackClass;
    //kafka consumer订阅topic partition
    private List<TopicPartition> subscribed;

    //标识是否在重新导入配置
    AtomicBoolean isReconfig = new AtomicBoolean(false);
    //kafka multi app 配置
    Properties config;
    //新配置
    Properties newConfig;

    public MessageFetcher(Properties config) {
        super(config.getProperty(AppConfig.APPNAME) + "'s consumer fetcher thread");

        this.config = config;
        pollTimeout = Long.valueOf(config.getProperty(AppConfig.MESSAGEFETCHER_POLL_TIMEOUT));
        enableRetry = Boolean.valueOf(config.getProperty(AppConfig.MESSAGEFETCHER_COMMIT_ENABLERETRY));
        maxRetry = Integer.valueOf(config.getProperty(AppConfig.MESSAGEFETCHER_COMMIT_MAXRETRY));

        //messagehandler.mode => OPOT/OPMT
        AbstractMessageHandlersManager.MsgHandlerManagerModel model = AbstractMessageHandlersManager.MsgHandlerManagerModel.getByDesc(
                config.getProperty(AppConfig.MESSAGEHANDLERMANAGER_MODEL));

        switch (model){
            case OPOT:
                this.handlersManager = new OPOTMessageHandlersManager(config);
                break;
            case OPMT:
                this.handlersManager = new OPMTMessageHandlersManager(config);
                break;
            case OPMT2:
                this.handlersManager = new OPMT2MessageHandlersManager(config);
                break;
            default:
                this.handlersManager = null;
                throw new IllegalStateException("init message handler manager error");
        }

        callBackClass = ClassUtils.getClass(config.getProperty(AppConfig.MESSAGEFETCHER_CONSUME_CALLBACK));

        this.consumer = new KafkaConsumer<K, V>(config);
        this.consumer.subscribe(AppConfigUtils.getSubscribeTopic(config), new MessageFetcher.InnerConsumerRebalanceListener<>(this));
    }

    @Override
    public void close(){
        log.info("consumer fetcher thread closing...");
        this.isStopped = true;
        this.interrupt();
    }

    @Override
    public Properties getConfig() {
        return config;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public MessageHandlersManager getHandlersManager() {
        return handlersManager;
    }

    @Override
    public void run() {
        long dida = 0;
        long maxDida = 10;
        long offset = -1;
        log.info("consumer fetcher thread started");
        try{
            while(!isStopped && !Thread.currentThread().isInterrupted()){
                //查看有没offset需要提交
                Map<TopicPartition, OffsetAndMetadata> offsets = allPendingOffsets();

                if(offsets != null){
                    //有offset需要提交
                    log.info("consumer commit [" + offsets.size() + "] topic partition offsets");
                    commitOffsetsSync(offsets);
                }

                //重新导入配置中....
                //会停止接受消息(因为consumer与broker存在session timout,该过程要尽量快)
                //message handler会停止处理消息(OPMT除外),同时只会清理资源减少(存在涉及同步问题,所以要停止消息处理),资源增加会在dispatcher时进行
                if(isReconfig.get()){
                    log.info("kafka consumer pause receive all records");
                    //提交队列中待提交的Offsets
                    Map<TopicPartition, OffsetAndMetadata> topicPartition2Offset = allPendingOffsets();
                    commitOffsetsSync(topicPartition2Offset);
                    //停止消费消息
                    consumer.pause(subscribed);
                    consumer.poll(0);
                    //在kafka consumer线程更新配置
                    doUpdateConfig(newConfig);
                }

                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);

                //空闲n久就开始输出.....
                if(records == null || records.count() < 0){
                    dida ++;
                    if(dida > maxDida){
                        System.out.println(".");
                    }
                }
                else {
                    dida = 0;
                }

                if(subscribed == null || subscribed.size() < 0){
                    subscribed = new ArrayList<>(consumer.assignment());
                }
                for(TopicPartition topicPartition: records.partitions())
                    for(ConsumerRecord<K, V> record: records.records(topicPartition)){
                        if(record.offset() > offset){
                            offset = record.offset();
                        }
                        //按照某种策略提交线程处理
                        CallBack callBack = callBackClass != null? ClassUtils.instance(callBackClass) : null;
                        if(callBack != null){
                            try {
                                callBack.setup(config, this);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        handlersManager.dispatch(new ConsumerRecordInfo(record, callBack), pendingOffsets);
                    }
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

    public List<TopicPartition> getSubscribed() {
        return subscribed;
    }

    public long position(TopicPartition topicPartition){
        return consumer.position(topicPartition);
    }

    private void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets){
        if(offsets == null || offsets.size() <= 0){
            return;
        }

        log.info("consumer commit offsets Sync...");
        consumer.commitSync(offsets);
        Statistics.instance().append("offset", TPStrUtils.topicPartitionOffsetsStr(offsets) + System.lineSeparator());
        log.info("consumer offsets [" + TPStrUtils.topicPartitionOffsetsStr(offsets) + "] committed");
    }

    private void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets){
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
                    log.info("consumer offsets [" + TPStrUtils.topicPartitionOffsetsStr(offsets) + "] committed");
                }
            }
        });
    }

    private void commitOffsetsSyncWhenRebalancing(){
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

    /**
     * 异步更新配置(仅仅设置标识),但要stop the world并保持kafka连接
     * @param config 新配置
     */
    @Override
    public void reConfig(Properties newConfig) {
        log.info("message fetcher reconfiging...");
        //在更新配置中,等待上次配置更新完,在执行此次更新
        while(!isReconfig.compareAndSet(false, true)){
            log.warn("message fetcher last reconfig still running!!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        while(!((AbstractMessageHandlersManager)handlersManager).updateReConfigStatus(false, true)){
            log.warn("message handlers manager last reconfig still running!!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        this.newConfig = newConfig;
    }

    private void doUpdateConfig(Properties newConfig){
        if(AppConfigUtils.isConfigItemChange(config, newConfig, AppConfig.MESSAGEFETCHER_POLL_TIMEOUT)){
            long pollTimeout = this.pollTimeout;
            this.pollTimeout = Long.valueOf(newConfig.getProperty(AppConfig.MESSAGEFETCHER_POLL_TIMEOUT));

            if(this.pollTimeout < 0){
                throw new IllegalStateException("config '" + AppConfig.MESSAGEFETCHER_POLL_TIMEOUT + "' state wrong");
            }

            log.info("config '" + AppConfig.MESSAGEFETCHER_POLL_TIMEOUT + "' change from '" + pollTimeout + "' to '" + this.pollTimeout + "'");
        }

        if(AppConfigUtils.isConfigItemChange(config, newConfig, AppConfig.MESSAGEFETCHER_COMMIT_MAXRETRY)){
            int maxRetry = this.maxRetry;
            this.maxRetry = Integer.valueOf(newConfig.getProperty(AppConfig.MESSAGEFETCHER_COMMIT_MAXRETRY));

            if(this.maxRetry < 0){
                throw new IllegalStateException("config '" + AppConfig.MESSAGEFETCHER_COMMIT_MAXRETRY + "' state wrong");
            }

            //重置当前retry次数
            nowRetry = 0;
            log.info("config '" + AppConfig.MESSAGEFETCHER_COMMIT_MAXRETRY + "' change from '" + maxRetry + "' to '" + this.maxRetry + "'");

        }

        //同步更新message handler manager
        handlersManager.reConfig(newConfig);

        //恢复接受已订阅分区的消息
        consumer.resume(subscribed);

        config = newConfig;
        this.newConfig = null;
        reConfigCompleted();

        log.info("message fetcher reconfiged");
    }

    private void reConfigCompleted(){
        while(!((AbstractMessageHandlersManager)handlersManager).updateReConfigStatus(true, false)){
            log.warn("something wrong! message handlers manager reconfig completed but still reconfiging!!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        while(!this.isReconfig.compareAndSet(true, false)){
            log.warn("something wrong! message fetcher reconfig completed but still reconfiging!!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Created by 健勤 on 2017/7/25.
     * 在poll时调用,会停止receive 消息
     *
     * 并不能保证多个实例不会重复消费消息
     * 感觉只有每个线程一个消费者才能做到,不然消费了的消息很难做到及时提交Offset并且其他实例还没有启动
     *
     * 该监听器的主要目的是释放那些无用资源
     */
    static class InnerConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {
        protected static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(InnerConsumerRebalanceListener.class);
        protected final MessageFetcher<K, V> messageFetcher;
        protected Set<TopicPartition> beforeAssignedTopicPartition;
        //jvm内存缓存目前消费到的Offset
        private Map<TopicPartition, Long> topicPartition2Offset = new HashMap<>();

        public InnerConsumerRebalanceListener(MessageFetcher<K, V> messageFetcher) {
            this.messageFetcher = messageFetcher;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            log.debug("kafka consumer onPartitionsRevoked...");
            //刚启动
            if(collection == null){
                return;
            }

            //设置标识,禁止message fetcher dispatch 消息
            messageFetcher.getHandlersManager().consumerRebalanceNotify(new HashSet<TopicPartition>(collection));
            //保存之前分配到的TopicPartition
            beforeAssignedTopicPartition = new HashSet<>(collection);
            //提交最新处理完的Offset
            messageFetcher.commitOffsetsSyncWhenRebalancing();

            //缓存当前Consumer poll到的Offset
            if(collection != null && collection.size() > 0){
                log.info("consumer origin assignment: " + TPStrUtils.topicPartitionsStr(collection));
                log.info("consumer rebalancing...");
                //保存在jvm内存中
                for(TopicPartition topicPartition: (Collection<TopicPartition>)collection){
                    topicPartition2Offset.put(topicPartition, messageFetcher.position(topicPartition));
                }
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection)
        {
            log.debug("kafka consumer onPartitionsAssigned!!!");

            //获取之前分配到但此次又没有分配到的TopicPartitions
            beforeAssignedTopicPartition.removeAll(collection);
            //还需清理已经交给handler线程
            messageFetcher.getHandlersManager().doOnConsumerReAssigned(beforeAssignedTopicPartition);
            //再一次提交之前分配到但此次又没有分配到的TopicPartition对应的最新Offset
            messageFetcher.commitOffsetsSyncWhenRebalancing();
            beforeAssignedTopicPartition = null;

            //重置consumer position并reset缓存
            if(collection != null && collection.size() > 0){
                for(TopicPartition topicPartition: (Collection<TopicPartition>)collection){
                    Long nowOffset = topicPartition2Offset.get(topicPartition);
                    if(nowOffset != null){
                        messageFetcher.consumer.seek(topicPartition, nowOffset);
                        log.info(topicPartition.topic() + "-" + topicPartition.partition() + "'s Offset seek to " + nowOffset);
                    }
                }
                log.info("consumer reassigned");
                log.info("consumer new assignment: " + TPStrUtils.topicPartitionsStr(collection));
                //清理offset缓存
                topicPartition2Offset.clear();
            }
            Statistics.instance().append("offset", System.lineSeparator());
        }

    }

}
