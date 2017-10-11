package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.MessageHandler;
import org.kin.kafka.multithread.common.DefaultThreadFactory;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.kin.kafka.multithread.common.ConsumerRecordInfo;
import org.kin.kafka.multithread.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by hjq on 2017/7/4.
 * OPMT ==> one partition more thread
 * 貌似这样子的设计CommitStrategy无法使用
 * 不能保证message handler线程安全
 *
 * 似乎这种方法是通用实现,只要把线程池线程数设置为1,那么就单线程版本咯
 * 虽然是通用版本,但是大量的线程切换导致性能开销
 *
 * 创建多个message handler实例,负载均衡地处理所有dispatch的信息
 *
 * 潜在问题:
 *  1.存在位置问题,poll()偶然长时间阻塞而导致session timeout的可能超时
 *  后面加一个log.info就OK(有时候还不行),有点不明觉厉
 *  解决:怀疑是和本地系统磁盘长时间100%有关
 *
 *  还是使用OPOT版本,可承受高负载,多开几个实例就好了.
 *
 */
public class OPMTMessageHandlersManager extends AbstractMessageHandlersManager {
    private static final Logger log = LoggerFactory.getLogger(OPMTMessageHandlersManager.class);
    /**
     * keepalive为60s,目的是尽可能充分利用系统资源,因为在OPMT模式下,处理线程是动态的,在高负载且系统能够撑住的情况下,充分利用线程资源处理消息
     */
    private Map<TopicPartition, ThreadPoolExecutor> topicPartition2Pools = new HashMap<>();
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new HashMap<>();
    private Map<TopicPartition, List<MessageHandler>> topicPartition2MessageHandlers = new HashMap<>();

    //用于负载均衡,负载线程池每一个线程
    private Map<TopicPartition, Long> topicPartition2Counter = new HashMap<>();
    //有需要关闭线程池时,缓存当前的所有任务,后续再次启动线程池时再添加至任务队列
    //包括被线程池中断的正在执行的message
    private Map<TopicPartition, List<Runnable>> topicPartition2UnHandledRunnableCache = new ConcurrentHashMap<>();

    //定期更新pendingwindow的窗口,以防过长的有效连续的Offset队列
    /**
     * 因为PendingWindow的Offset commit更新操作是MessageHandlerThread处理,如果当前没有MessageHandlerThread非阻塞运行,且PendingWindow缓存着大量待提交的Offset
     * 如果此时出现系统故障,大量完成的record因为没有提交Offset而需要重新处理
     * 所以通过定时抢占来完成PendingWindow的Offset commit更新操作以减少不必要的record重复处理
     */
    private Timer updatePengdingWindowAtFixRate;

    private int handlerSize;
    private int maxThreadSizePerPartition;
    private int minThreadSizePerPartition;
    private int threadQueueSizePerPartition;
    private int slidingWindow;

    public OPMTMessageHandlersManager() {
        super(AppConfig.DEFAULT_APPCONFIG);
    }

    public OPMTMessageHandlersManager(Properties config){
        super(config);
        this.handlerSize = Integer.valueOf(config.get(AppConfig.OPMT_HANDLERSIZE).toString());
        this.minThreadSizePerPartition = Integer.valueOf(config.getProperty(AppConfig.OPMT_MINTHREADSIZEPERPARTITION));
        this.maxThreadSizePerPartition = Integer.valueOf(config.getProperty(AppConfig.OPMT_MAXTHREADSIZEPERPARTITION));
        this.threadQueueSizePerPartition = Integer.valueOf(config.getProperty(AppConfig.OPMT_THREADQUEUESIZEPERPARTITION));
        this.slidingWindow = Integer.valueOf(config.getProperty(AppConfig.PENDINGWINDOW_SLIDINGWINDOW));
    }

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets) {
        log.debug("dispatching message: " + TPStrUtils.consumerRecordDetail(consumerRecordInfo.record()));

        //自动提交的情况下,不会使用pendingwindow,进而没必要启动一条定时线程来检索空的map实例
        if(!isAutoCommit && updatePengdingWindowAtFixRate == null){
            updatePengdingWindowAtFixRate = new Timer("updatePengdingWindowAtFixRate");
            //每5秒更新pendingwindow的有效连续的Offset队列,也就是提交
            updatePengdingWindowAtFixRate.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    //不是更新配置或Rebalance的时候更新pendingwindow
                    if(!isRebalance.get() && !isReconfig.get()){
                        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
                            pendingWindow.commitFinished(null);
                        }
                    }
                }
            }, 0, 5000);
        }

        if(isRebalance.get()){
            log.debug("dispatch failure due to rebalancing...");
            return false;
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();

        ThreadPoolExecutor pool = topicPartition2Pools.get(topicPartition);
        if(pool == null){
            //消息处理线程池还没启动,则启动并绑定
            log.info("no thread pool cache, new one(MaxPoolSize = " + maxThreadSizePerPartition + ", QueueSize = "  + threadQueueSizePerPartition + ")");
            pool = new ThreadPoolExecutor(
                    minThreadSizePerPartition,
                    maxThreadSizePerPartition,
                    60,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(this.threadQueueSizePerPartition),
                    new DefaultThreadFactory(config.getProperty(AppConfig.APPNAME), "MessageHandler")
            );
            topicPartition2Pools.put(topicPartition, pool);
        }

        PendingWindow pendingWindow = topicPartition2PendingWindow.get(topicPartition);
        if(!isAutoCommit && pendingWindow == null){
            log.info("new pending window");
            //等待offset连续完整窗口还没创建,则新创建
            pendingWindow = new PendingWindow(slidingWindow, pendingOffsets);
            topicPartition2PendingWindow.put(topicPartition, pendingWindow);
        }

        List<MessageHandler> messageHandlers = topicPartition2MessageHandlers.get(topicPartition);
        if(messageHandlers == null){
            //round选择message handler
            log.info("init message handlers(size = " + handlerSize + ")");
            messageHandlers = new ArrayList<>();
            for(int i = 0; i < handlerSize; i++){
                messageHandlers.add(newMessageHandler(topicPartition.topic()));
            }
            topicPartition2Counter.put(topicPartition, 1L);
            topicPartition2MessageHandlers.put(topicPartition, messageHandlers);
        }
        //只会存在增加资源的情况,较少资源意味着需要同步,所以在stop the world的时候处理掉
        if(messageHandlers.size() < handlerSize){
            log.info("add message handlers(size = " + (handlerSize - messageHandlers.size()) + ")");
            for(int i = messageHandlers.size(); i < handlerSize; i++){
                messageHandlers.add(newMessageHandler(topicPartition.topic()));
            }
        }

        //round选择message handler
        MessageHandler handler = messageHandlers.get((int)(topicPartition2Counter.get(topicPartition) % messageHandlers.size()));
        topicPartition2Counter.put(topicPartition, topicPartition2Counter.get(topicPartition) + 1);

        log.debug("message: " + TPStrUtils.consumerRecordDetail(consumerRecordInfo.record()) + " wrappered as task has submit");
        pool.execute(new MessageHandlerTask(handler, pendingWindow, consumerRecordInfo));

        return true;
    }

    @Override
    public void consumerCloseNotify() {
        if(updatePengdingWindowAtFixRate != null){
            updatePengdingWindowAtFixRate.cancel();
        }

        //提交已完成处理的消息的最大offset
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            pendingWindow.commitLatest(false);
        }

        log.info("shutdown task thread pools...");
        //关闭线程池
        for(ThreadPoolExecutor pool: topicPartition2Pools.values()){
            //再关闭
            pool.shutdownNow();
        }
    }

    @Override
    public void consumerRebalanceNotify(Set<TopicPartition> topicPartitions) {
        isRebalance.set(true);

        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            //提交已完成处理的消息的最大offset
            pendingWindow.commitLatest(false);
        }
    }

    @Override
    public void doOnConsumerReAssigned(Set<TopicPartition> topicPartitions) {
        log.info("clean up target tasks");
        List<ThreadPoolExecutor> pools = new ArrayList<>();
        //清空线程池任务
        for(TopicPartition topicPartition: topicPartitions){
            //先清空队列
            topicPartition2Pools.get(topicPartition).getQueue().clear();
            pools.add(topicPartition2Pools.get(topicPartition));
        }

        if(pools.size() > 0){
            //等待正在处理的线程处理完
            log.info("waiting active task to finish and choose to cache thread pools...");
            int count = 0;
            while(count < 5){
                boolean isActive = false;

                for(ThreadPoolExecutor pool: pools){
                    if(!pool.isTerminated()){
                        isActive = true;
                    }
                }

                if(isActive){
                    count ++;
                    try {
                        Thread.currentThread().sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    break;
                }
            }

            //超过10s,强制关闭仍然在执行的现有资源
            if(count >= 5){
                log.info("waitting time out, force shutdown target active thread pool");
                for(ThreadPoolExecutor pool: pools){
                    pool.shutdownNow();
                }
            }
        }

        if(!isAutoCommit){
            log.info("clean up target used pending window");
            for(TopicPartition topicPartition: topicPartitions){
                //提交已完成处理的消息的最大offset
                topicPartition2PendingWindow.get(topicPartition).commitLatest(false);
                topicPartition2PendingWindow.remove(topicPartition);
            }
        }

        isRebalance.set(false);
    }

    @Override
    public void reConfig(Properties newConfig) {
        log.info("OPMT message handler manager reconfiging...");
        Integer minThreadSizePerPartition = this.minThreadSizePerPartition;
        Integer maxThreadSizePerPartition = this.maxThreadSizePerPartition;
        Integer threadQueueSizePerPartition = this.threadQueueSizePerPartition;
        Integer handlerSize = this.handlerSize;

        if(AppConfigUtils.isConfigItemChange(String.valueOf(minThreadSizePerPartition), newConfig, AppConfig.OPMT_MINTHREADSIZEPERPARTITION)){
            minThreadSizePerPartition = Integer.valueOf(newConfig.getProperty(AppConfig.OPMT_MINTHREADSIZEPERPARTITION));

            if(minThreadSizePerPartition <= 0){
                throw new IllegalStateException("config '" + AppConfig.OPMT_MINTHREADSIZEPERPARTITION + "' state wrong");
            }
        }

        if(AppConfigUtils.isConfigItemChange(String.valueOf(maxThreadSizePerPartition), newConfig, AppConfig.OPMT_MAXTHREADSIZEPERPARTITION)){
            maxThreadSizePerPartition = Integer.valueOf(newConfig.getProperty(AppConfig.OPMT_MAXTHREADSIZEPERPARTITION));

            if(maxThreadSizePerPartition <= 0){
                throw new IllegalStateException("config '" + AppConfig.OPMT_MAXTHREADSIZEPERPARTITION + "' state wrong");
            }
        }
        if(AppConfigUtils.isConfigItemChange(String.valueOf(threadQueueSizePerPartition), newConfig, AppConfig.OPMT_THREADQUEUESIZEPERPARTITION)){
            threadQueueSizePerPartition = Integer.valueOf(newConfig.getProperty(AppConfig.OPMT_THREADQUEUESIZEPERPARTITION));

            if(threadQueueSizePerPartition < 0){
                throw new IllegalStateException("config '" + AppConfig.OPMT_THREADQUEUESIZEPERPARTITION + "' state wrong");
            }
        }
        if(AppConfigUtils.isConfigItemChange(String.valueOf(handlerSize), newConfig, AppConfig.OPMT_HANDLERSIZE)){
            handlerSize = Integer.valueOf(newConfig.getProperty(AppConfig.OPMT_HANDLERSIZE));

            if(handlerSize < 0){
                throw new IllegalStateException("config '" + AppConfig.OPMT_HANDLERSIZE + "' state wrong");
            }

            log.info("config '" + AppConfig.OPMT_HANDLERSIZE + "' change from '" + this.handlerSize + "' to '" + handlerSize + "'");
        }

        boolean isThreadPoolClosed = false;


        //更新message handler的数量
        //需要关闭线程池
        boolean isHandlerSizeReduced = false;
        if(this.handlerSize > handlerSize){
            isThreadPoolClosed = true;
            isHandlerSizeReduced = true;
            log.info("reduce message handlers(size = " + (this.handlerSize - handlerSize) + ")");
            //收集不需要使用的messagehandler的hashcode
            //移除前n个
            HashSet<MessageHandler> discardedHandlers = new HashSet<>();
            for(int i = 0; i < this.handlerSize - handlerSize; i++){
                for(TopicPartition key: topicPartition2MessageHandlers.keySet()){
                    List<MessageHandler> messageHandlers = topicPartition2MessageHandlers.get(key);
                    MessageHandler oldMessageHandler = messageHandlers.remove(0);
                    discardedHandlers.add(oldMessageHandler);
                }
            }

            //round-robin地将余下message handler替代被移除的message handler
            for(TopicPartition key: topicPartition2MessageHandlers.keySet()){
                List<MessageHandler> messageHandlers = topicPartition2MessageHandlers.get(key);

                int round = 0;

                for(Runnable task: topicPartition2Pools.get(key).shutdownNow()){
                    MessageHandlerTask wrapperTask = (MessageHandlerTask) task;
                    if(discardedHandlers.contains(wrapperTask.handler)){
                        wrapperTask.handler = messageHandlers.get((round++) % messageHandlers.size());
                    }
                    if(!topicPartition2UnHandledRunnableCache.containsKey(key)){
                        topicPartition2UnHandledRunnableCache.put(key, new ArrayList<>());
                    }
                    topicPartition2UnHandledRunnableCache.get(key).add(wrapperTask);
                }
            }

            //释放被丢弃的message handler占用的资源
            for(MessageHandler discardedMessageHandler: discardedHandlers){
                try {
                    discardedMessageHandler.cleanup();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        this.handlerSize = handlerSize;

        //需要关闭线程池
        if(this.minThreadSizePerPartition != minThreadSizePerPartition ||
                this.maxThreadSizePerPartition != maxThreadSizePerPartition ||
                this.threadQueueSizePerPartition != threadQueueSizePerPartition){
            if(minThreadSizePerPartition > 0 &&
                    maxThreadSizePerPartition > 0 &&
                    threadQueueSizePerPartition > 0 &&
                    minThreadSizePerPartition <= Integer.MAX_VALUE &&
                    maxThreadSizePerPartition <= Integer.MAX_VALUE &&
                    threadQueueSizePerPartition <= Integer.MAX_VALUE){
                if(minThreadSizePerPartition <= maxThreadSizePerPartition){
                    //如果handler size没有减少,那么意味着不会关闭线程,此处要缓存所有待执行任务,后续重新启动线程池再提交
                    if(!isHandlerSizeReduced){
                        isThreadPoolClosed = true;
                        for(TopicPartition topicPartition: topicPartition2Pools.keySet()){
                            ThreadPoolExecutor nowPool = topicPartition2Pools.get(topicPartition);
                            //转换到pool的STOP的状态,该状态时仅仅会处理完pool的线程就转到TERMINAL
                            List<Runnable> originTasks = nowPool.shutdownNow();
                            if(topicPartition2UnHandledRunnableCache.containsKey(topicPartition)){
                                topicPartition2UnHandledRunnableCache.get(topicPartition).addAll(originTasks);
                            }
                            else{
                                topicPartition2UnHandledRunnableCache.put(topicPartition, originTasks);
                            }
                        }
                    }

                    log.info("config '" + AppConfig.OPMT_MINTHREADSIZEPERPARTITION + "' change from '" + this.minThreadSizePerPartition + "' to '" + minThreadSizePerPartition + "'");
                    log.info("config '" + AppConfig.OPMT_MAXTHREADSIZEPERPARTITION + "' change from '" + this.maxThreadSizePerPartition + "' to '" + maxThreadSizePerPartition + "'");
                    log.info("config '" + AppConfig.OPMT_THREADQUEUESIZEPERPARTITION + "' change from '" + this.threadQueueSizePerPartition + "' to '" + threadQueueSizePerPartition + "'");

                    this.minThreadSizePerPartition = minThreadSizePerPartition;
                    this.maxThreadSizePerPartition = maxThreadSizePerPartition;
                    this.threadQueueSizePerPartition = threadQueueSizePerPartition;
                }
                else{
                    throw new IllegalStateException("config '" + AppConfig.OPMT_MINTHREADSIZEPERPARTITION + "' and '" + AppConfig.OPMT_MAXTHREADSIZEPERPARTITION + "' state wrong");
                }
            }
            else{
                throw new IllegalStateException("config args '" + AppConfig.OPMT_MINTHREADSIZEPERPARTITION + "' or '" + AppConfig.OPMT_MAXTHREADSIZEPERPARTITION + "' or '" + AppConfig.OPMT_THREADQUEUESIZEPERPARTITION + "' state wrong");
            }
        }
        else{
            log.info("handler thread pool doesn't change");
        }

        //如果线程池关闭,则重新启动
        if(isThreadPoolClosed){
            log.info("thread pool closed per topic partition, so restart now");
            for(TopicPartition topicPartition: topicPartition2Pools.keySet()){
                ThreadPoolExecutor newPool = new ThreadPoolExecutor(
                        minThreadSizePerPartition,
                        maxThreadSizePerPartition,
                        60,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(threadQueueSizePerPartition),
                        new DefaultThreadFactory(config.getProperty(AppConfig.APPNAME), "MessageHandler")
                );
                //根据Offset排序,以免不连续Offset提交
                topicPartition2UnHandledRunnableCache.get(topicPartition).sort(new Comparator<Runnable>() {
                    @Override
                    public int compare(Runnable o1, Runnable o2) {
                        if(o1 instanceof MessageHandlerTask && o2 instanceof MessageHandlerTask){
                            MessageHandlerTask task1 = (MessageHandlerTask) o1;
                            MessageHandlerTask task2 = (MessageHandlerTask) o2;

                            long offset1 = task1.target.record().offset();
                            long offset2 = task2.target.record().offset();

                            return offset1 > offset2 ? 1 : offset1 == offset2 ? 0 : -1;
                        }
                        return -1;
                    }
                });

                for(Runnable runnable: topicPartition2UnHandledRunnableCache.get(topicPartition)){
                    newPool.execute(runnable);
                }
                topicPartition2Pools.put(topicPartition, newPool);
            }
            topicPartition2UnHandledRunnableCache.clear();
        }

        //更新pendingwindow的配置
        if(AppConfigUtils.isConfigItemChange(String.valueOf(slidingWindow), newConfig, AppConfig.PENDINGWINDOW_SLIDINGWINDOW)){
            int slidingWindow = this.slidingWindow;

            //不需要同步,因为stop the world(处理线程停止处理消息)
            this.slidingWindow = Integer.valueOf(newConfig.get(AppConfig.PENDINGWINDOW_SLIDINGWINDOW).toString());

            log.info("config '" + AppConfig.PENDINGWINDOW_SLIDINGWINDOW + "' change from '" + slidingWindow + "' to '" + this.slidingWindow + "'");

            for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
                pendingWindow.reConfig(newConfig);
            }
        }
        log.info("OPMT message handler manager reconfiged");
    }

    private final class MessageHandlerTask implements Runnable{
        private Logger log = LoggerFactory.getLogger(MessageHandlerTask.class);
        private MessageHandler handler;
        private PendingWindow pendingWindow;
        private ConsumerRecordInfo target;

        public MessageHandlerTask(MessageHandler handler, PendingWindow pendingWindow, ConsumerRecordInfo target) {
            this.handler = handler;
            this.pendingWindow = pendingWindow;
            this.target = target;
        }

        @Override
        public void run() {
            try {
                log.debug(Thread.currentThread().getName() + " start to handle task... [" + target + "]");
                handler.handle(target.record());
                if(!isAutoCommit){
                    pendingWindow.commitFinished(target);
                }
                log.debug(Thread.currentThread().getName() + " has finished handling task ");
            } catch (Exception e) {
                if(e instanceof InterruptedException){
                    log.info("unfinished task(" + target.topicPartition().topic() + "-" + target.topicPartition().partition() + ", offset=" + target.record().offset() + ") interrupted, ready to restart");
                    if(topicPartition2UnHandledRunnableCache.containsKey(target.topicPartition())){
                        topicPartition2UnHandledRunnableCache.get(target.topicPartition()).add(this);
                    }
                    else{
                        List<Runnable> runnables = new ArrayList<>();
                        runnables.add(this);
                        topicPartition2UnHandledRunnableCache.put(target.topicPartition(), runnables);
                    }
                }
                else {
                    e.printStackTrace();
                }
            }
        }
    }

}
