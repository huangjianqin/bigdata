package org.kin.kafka.consumer.multi;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.kin.framework.log.LoggerOprs;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.StringUtils;
import org.kin.framework.utils.SysUtils;

import java.util.*;

/**
 * @author huangjianqin
 * @date 2020/8/14
 */
public class KafkaFetchConfig<K, V> implements LoggerOprs {
    private static final String ALL = "topic@all";

    /** kafka broker 地址 */
    private String bootstrapServer = "localhost:9092";
    /** kafka consumer group id */
    private String groupId = " kafkaFetcher";
    /** kafka client 自动提交Offset */
    private boolean autoCommit = true;
    /**
     * 订阅topic
     * 动态topic,topic,
     * 静态topic-par,topic-par(不支持)
     */
    private String subscribe;
    /** kafka offset */
    private String offset = "";
    /** kafka client poll timeout */
    private long pollTimeout = 1000;
    /** opmt 模式下, fetcher异步提交offset最大尝试次数 */
    private int maxRetry = 5;
    /**
     * OPMT -> 每个topic partition并发数
     * OCOT -> kafka consumer 数量
     */
    private int parallelism = SysUtils.CPU_NUM / 2 + 1;
    /** n轮poll没有获取消息则预警 */
    private int idleTimesAlarm = 5;
    /** 滑动窗口大小 */
    private int windowSize = 1000;
    /** key -> topic, value -> MessageHandler class */
    private Map<String, Class<? extends KafkaMessageHandler>> topic2HandlerClass = new HashMap<>();
    /** key -> topic, value -> CommitStrategy class */
    private Map<String, Class<? extends OffsetCommitStrategy>> topic2CommitStrategyClass = new HashMap<>();
    /** kafka consumer reance listener实现 */
    private Class<? extends AbstractConsumerRebalanceListener> consumerRebalanceListenerClass = JvmConsumerRebalanceListener.class;

    //-----------------------------------------------------------------对外api----------------------------------------------------------------------
    public static <K, V> KafkaFetchConfig<K, V> of() {
        return new KafkaFetchConfig<>();
    }

    public static <K, V> KafkaFetchConfig<K, V> of(String bootstrapServer) {
        KafkaFetchConfig<K, V> fetchConfig = new KafkaFetchConfig<>();
        fetchConfig.bootstrapServer = bootstrapServer;
        return fetchConfig;
    }

    public KafkaFetchConfig<K, V> groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaFetchConfig<K, V> autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public KafkaFetchConfig<K, V> subscribe(String subscribe) {
        this.subscribe = subscribe;
        return this;
    }

    public KafkaFetchConfig<K, V> offset(String offset) {
        this.offset = offset;
        return this;
    }

    public KafkaFetchConfig<K, V> pollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
        return this;
    }

    public KafkaFetchConfig<K, V> maxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
        return this;
    }

    public KafkaFetchConfig<K, V> parallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public KafkaFetchConfig<K, V> idleTimesAlarm(int idleTimesAlarm) {
        this.idleTimesAlarm = idleTimesAlarm;
        return this;
    }

    public KafkaFetchConfig<K, V> windowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public KafkaFetchConfig<K, V> handler(Class<? extends KafkaMessageHandler> claxx) {
        this.topic2HandlerClass.put(ALL, claxx);
        return this;
    }

    public KafkaFetchConfig<K, V> handler(String topic, Class<? extends KafkaMessageHandler> claxx) {
        this.topic2HandlerClass.put(topic, claxx);
        return this;
    }

    public KafkaFetchConfig<K, V> offsetCommitStrategy(Class<? extends OffsetCommitStrategy> claxx) {
        this.topic2CommitStrategyClass.put(ALL, claxx);
        return this;
    }

    public KafkaFetchConfig<K, V> offsetCommitStrategy(String topic, Class<? extends OffsetCommitStrategy> claxx) {
        this.topic2CommitStrategyClass.put(topic, claxx);
        return this;
    }

    public KafkaFetchConfig<K, V> rebalanceListener(Class<? extends AbstractConsumerRebalanceListener> claxx) {
        this.consumerRebalanceListenerClass = claxx;
        return this;
    }

    private void comonCheck() {
        Preconditions.checkArgument(StringUtils.isNotBlank(bootstrapServer), "kafka bootstrap server must not blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(subscribe), "consumer subscribe topics must not black");
    }

    /**
     * 以ocot形式开始fetch
     */
    public void ocot() {
        comonCheck();

        OCOTMultiProcessor<K, V> ocotMultiProcessor = new OCOTMultiProcessor<>(this);
        ocotMultiProcessor.runFetch();
    }

    /**
     * 以opmt形式开始fetch
     */
    public void opmt() {
        comonCheck();

        KafkaMessageFetcher<K, V> kafkaMessageFetcher = new KafkaMessageFetcher<>(this);
        kafkaMessageFetcher.runFetch();
    }

    //-----------------------------------------------------------------内部接口----------------------------------------------------------------------
    Properties toKafkaConfig() {
        Properties cfg = new Properties();
        cfg.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        cfg.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        cfg.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        cfg.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        cfg.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(autoCommit));
        return cfg;
    }

    boolean enableRetry() {
        return getMaxRetry() > 0;
    }

    /**
     * 通过class信息实例化Message handler并调用setup方法进行初始化
     */
    KafkaMessageHandler<K, V> constructMessageHandler(String topic) {
        //如果topic有指定
        Class<? extends KafkaMessageHandler> claxx = topic2HandlerClass.get(topic);
        if (Objects.isNull(claxx)) {
            //回退到全局配置
            claxx = topic2HandlerClass.get(ALL);
        }
        if (Objects.isNull(claxx)) {
            //回退到默认
            claxx = KafkaMessageConsumeCounter.class;
        }

        KafkaMessageHandler<K, V> kafkaMessageHandler = ClassUtils.instance(claxx);
        //初始化message handler
        kafkaMessageHandler.setup(this);

        return kafkaMessageHandler;
    }

    /**
     * 通过class信息实例化Commit strategy并调用setup方法进行初始化
     */
    OffsetCommitStrategy<K, V> constructCommitStrategy(String topic) {
        //如果topic有指定
        Class<? extends OffsetCommitStrategy> claxx = topic2CommitStrategyClass.get(topic);
        if (Objects.isNull(claxx)) {
            //回退到全局配置
            claxx = topic2CommitStrategyClass.get(ALL);
        }
        if (Objects.isNull(claxx)) {
            //回退到默认
            claxx = FixRateOffsetCommitStrategy.class;
        }

        OffsetCommitStrategy<K, V> offsetCommitStrategy = ClassUtils.instance(claxx);
        //初始化CommitStrategy
        offsetCommitStrategy.setup(this);

        return offsetCommitStrategy;
    }

    List<String> topics() {
        return Arrays.asList(getSubscribe().split(","));
    }

    //getter
    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public String getGroupId() {
        return groupId;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public String getSubscribe() {
        return subscribe;
    }

    public String getOffset() {
        return offset;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getIdleTimesAlarm() {
        return idleTimesAlarm;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public Class<? extends AbstractConsumerRebalanceListener> getConsumerRebalanceListenerClass() {
        return consumerRebalanceListenerClass;
    }
}
