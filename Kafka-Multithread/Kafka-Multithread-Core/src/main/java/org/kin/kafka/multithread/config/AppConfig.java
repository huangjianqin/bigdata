package org.kin.kafka.multithread.config;

/**
 * Created by 健勤 on 2017/7/21.
 * kafka多线程工具配置类
 * 与Kafka-Multithread-config AppConfig相同
 *
 * 此处忽略kafka producer和consumer配置,因为启动对应实例时会自动识别参数是否满足
 */
public class AppConfig {
    //common
    public static final String APPNAME = "appName";
    public static final String APPHOST = "appHost";
    /**
     * RUN
     * CLOSE
     */
    public static final String APPSTATUS = "appStatus";

    //kafka consumer
    //暂停消费的分区,格式是topic-par,
    public static final String KAFKA_CONSUMER_PAUSE = "";
    //暂停消费的分区,格式是topic-par,
    public static final String KAFKA_CONSUMER_RESUME = "";
    //目前订阅的分区,不一定准确,可能在Rebalancing时还没更新
    public static final String KAFKA_CONSUMER_NOW_SUBSCRIBE = "";

    //message fetcher
    public static final String MESSAGEFETCHER_POLL_TIMEOUT = "messagefetcher.poll.timeout";
    public static final String MESSAGEFETCHER_COMMIT_MAXRETRY = "messagefetcher.commit.maxretry";
    public static final String MESSAGEFETCHER_CONSUME_CALLBACK = "messagefetcher.consume.callback";

    //message handler
    public static final String MESSAGEHANDLER_MODEL = "messagehandler.model";
    public static final String MESSAGEHANDLER_QUEUEPERTHREAD_POLLTIMEOUT = "messagehandler.queueperthread.polltimeout";

    //opot

    //opmt
    public static final String OPMT_THREADSIZEPERPARTITION = "opmt.threadsizeperpartition";
    public static final String OPMT_THREADQUEUESIZEPERPARTITION = "opmt.threadqueuesizeperpartition";
    public static final String OPMT_HANDLERSIZE = "opmt.handlersize";

    //opmt2
    public static final String OPMT2_THREADSIZEPERPARTITION = "opmt2.threadsizeperpartition";

    //ocot
    public static final String OCOT_CONSUMERNUM = "ocot.consumernum";

    //pendingwindow
    public static final String PENDINGWINDOW_SLIDINGWINDOW = "slidingWindow";

    //config fetcher
    public static final String CONFIGFETCHER_FETCHERINTERVAL = "configfetcher.fetcherinterval";

    //本地
    //配置中心节点信息
    public static final String CONFIGCENTER_HOST = "configcenter.host";
    public static final String CONFIGCENTER_PORT = "configcenter.port";
}
