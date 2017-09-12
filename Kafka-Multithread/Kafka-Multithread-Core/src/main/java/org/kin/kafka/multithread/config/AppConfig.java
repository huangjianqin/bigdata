package org.kin.kafka.multithread.config;

import java.util.Properties;

/**
 * Created by 健勤 on 2017/7/21.
 * kafka多线程工具配置类
 * 与Kafka-Multithread-config AppConfig相同
 *
 * 此处忽略kafka producer和consumer配置,因为启动对应实例时会自动识别参数是否满足
 */
public class AppConfig {
    public static final Properties DEFAULT_APPCONFIG = new Properties();

    //common
    //require
    public static final String APPNAME = "appName";
    //require
    public static final String APPHOST = "appHost";
    /**
     * RUN
     * CLOSE
     */
    //require
    public static final String APPSTATUS = "appStatus";

    //kafka consumer
    //暂停消费的分区,格式是topic-par,
    public static final String KAFKA_CONSUMER_PAUSE = "kafka.consumer.pause";
    //暂停消费的分区,格式是topic-par,
    public static final String KAFKA_CONSUMER_RESUME = "kafka.consumer.resume";
    //订阅topic
    public static final String KAFKA_CONSUMER_SUBSCRIBE = "kafka.consumer.subscribe";

    //message fetcher
    public static final String MESSAGEFETCHER_POLL_TIMEOUT = "messagefetcher.poll.timeout";
    public static final String MESSAGEFETCHER_COMMIT_MAXRETRY = "messagefetcher.commit.maxretry";
    public static final String MESSAGEFETCHER_CONSUME_CALLBACK = "messagefetcher.consume.callback";

    //message handler
    //require
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

    //container
    /**
     * jvm,与app同一jvm
     * node
     */
    public static final String APP_CHILD_RUN_MODEL = "app.child.run.model";

    static {
        DEFAULT_APPCONFIG.put(AppConfig.KAFKA_CONSUMER_PAUSE, AppConfigValue.DEFAULT_KAFKA_CONSUMER_PAUSE);
        DEFAULT_APPCONFIG.put(AppConfig.KAFKA_CONSUMER_RESUME, AppConfigValue.DEFAULT_KAFKA_CONSUMER_RESUME);

        DEFAULT_APPCONFIG.put(AppConfig.MESSAGEFETCHER_COMMIT_MAXRETRY, AppConfigValue.DEFAULT_MESSAGEFETCHER_COMMIT_MAXRETRY);
        DEFAULT_APPCONFIG.put(AppConfig.MESSAGEFETCHER_CONSUME_CALLBACK, AppConfigValue.DEFAULT_MESSAGEFETCHER_CONSUME_CALLBACK);
        DEFAULT_APPCONFIG.put(AppConfig.MESSAGEFETCHER_POLL_TIMEOUT, AppConfigValue.DEFAULT_MESSAGEFETCHER_POLL_TIMEOUT);

        DEFAULT_APPCONFIG.put(AppConfig.MESSAGEHANDLER_QUEUEPERTHREAD_POLLTIMEOUT, AppConfigValue.DEFAULT_MESSAGEHANDLER_QUEUEPERTHREAD_POLLTIMEOUT);

        DEFAULT_APPCONFIG.put(AppConfig.OPMT_HANDLERSIZE, AppConfigValue.DEFAULT_OPMT_HANDLERSIZE);
        DEFAULT_APPCONFIG.put(AppConfig.OPMT_THREADQUEUESIZEPERPARTITION, AppConfigValue.DEFAULT_OPMT_THREADQUEUESIZEPERPARTITION);
        DEFAULT_APPCONFIG.put(AppConfig.OPMT_THREADSIZEPERPARTITION, AppConfigValue.DEFAULT_OPMT_THREADSIZEPERPARTITION);

        DEFAULT_APPCONFIG.put(AppConfig.OPMT2_THREADSIZEPERPARTITION, AppConfigValue.DEFAULT_OPMT2_THREADSIZEPERPARTITION);

        DEFAULT_APPCONFIG.put(AppConfig.OCOT_CONSUMERNUM, AppConfigValue.DEFAULT_OCOT_CONSUMERNUM);

        DEFAULT_APPCONFIG.put(AppConfig.PENDINGWINDOW_SLIDINGWINDOW, AppConfigValue.DEFAULT_PENDINGWINDOW_SLIDINGWINDOW);

        DEFAULT_APPCONFIG.put(AppConfig.CONFIGFETCHER_FETCHERINTERVAL, AppConfigValue.DEFAULT_CONFIGFETCHER_FETCHERINTERVAL);
    }
}
