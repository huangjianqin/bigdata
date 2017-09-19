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
     * UPDATE
     * CLOSE
     * RESTART
     */
    //require
    public static final String APPSTATUS = "appStatus";

    //kafka consumer
    //仅支持RESTART和RUN状态
    //require
    //订阅topic
    //动态topic,topic,
    //静态topic-par,topic-par(不支持)
    public static final String KAFKA_CONSUMER_SUBSCRIBE = "kafka.consumer.subscribe";

    //message fetcher
    public static final String MESSAGEFETCHER_POLL_TIMEOUT = "messagefetcher.poll.timeout";
    public static final String MESSAGEFETCHER_COMMIT_MAXRETRY = "messagefetcher.commit.maxretry";
    //仅支持RESTART和RUN状态
    public static final String MESSAGEFETCHER_CONSUME_CALLBACK = "messagefetcher.consume.callback";

    //message handler
    //require
    public static final String MESSAGEHANDLERMANAGER_MODEL = "messagehandlermanager.model";
    //仅支持RESTART和RUN状态
    //require
    public static final String MESSAGEHANDLER = "messagehandler";
    public static final String COMMITSTRATEGY = "commitstrategy";
    public static final String CONSUMERREBALANCELISTENER = "consumerrebalancelistener";

    //opot

    //opmt
    public static final String OPMT_MAXTHREADSIZEPERPARTITION = "opmt.maxthreadsizeperpartition";
    public static final String OPMT_MINTHREADSIZEPERPARTITION = "opmt.minthreadsizeperpartition";
    public static final String OPMT_THREADQUEUESIZEPERPARTITION = "opmt.threadqueuesizeperpartition";
    public static final String OPMT_HANDLERSIZE = "opmt.handlersize";

    //opmt2
    public static final String OPMT2_THREADSIZEPERPARTITION = "opmt2.threadsizeperpartition";

    //ocot
    public static final String OCOT_CONSUMERNUM = "ocot.consumernum";

    //pendingwindow
    public static final String PENDINGWINDOW_SLIDINGWINDOW = "slidingWindow";

    //本地
    //config fetcher
    public static final String CONFIGFETCHER_FETCHERINTERVAL = "configfetcher.fetcherinterval";

    //配置中心节点信息
    public static final String CONFIGCENTER_HOST = "configcenter.host";
    public static final String CONFIGCENTER_PORT = "configcenter.port";

    //container
    /**
     * jvm,与app同一jvm
     * node,同一节点,不同jvm
     */
    public static final String APP_CHILD_RUN_MODEL = "app.child.run.model";

    static {
        DEFAULT_APPCONFIG.put(AppConfig.MESSAGEFETCHER_COMMIT_MAXRETRY, DefaultAppConfig.DEFAULT_MESSAGEFETCHER_COMMIT_MAXRETRY);
        DEFAULT_APPCONFIG.put(AppConfig.MESSAGEFETCHER_CONSUME_CALLBACK, DefaultAppConfig.DEFAULT_MESSAGEFETCHER_CONSUME_CALLBACK);
        DEFAULT_APPCONFIG.put(AppConfig.MESSAGEFETCHER_POLL_TIMEOUT, DefaultAppConfig.DEFAULT_MESSAGEFETCHER_POLL_TIMEOUT);

        DEFAULT_APPCONFIG.put(AppConfig.OPMT_HANDLERSIZE, DefaultAppConfig.DEFAULT_OPMT_HANDLERSIZE);
        DEFAULT_APPCONFIG.put(AppConfig.OPMT_THREADQUEUESIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT_THREADQUEUESIZEPERPARTITION);
        DEFAULT_APPCONFIG.put(AppConfig.OPMT_MAXTHREADSIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT_MAXTHREADSIZEPERPARTITION);
        DEFAULT_APPCONFIG.put(AppConfig.OPMT_MINTHREADSIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT_MINTHREADSIZEPERPARTITION);

        DEFAULT_APPCONFIG.put(AppConfig.OPMT2_THREADSIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT2_THREADSIZEPERPARTITION);

        DEFAULT_APPCONFIG.put(AppConfig.OCOT_CONSUMERNUM, DefaultAppConfig.DEFAULT_OCOT_CONSUMERNUM);

        DEFAULT_APPCONFIG.put(AppConfig.PENDINGWINDOW_SLIDINGWINDOW, DefaultAppConfig.DEFAULT_PENDINGWINDOW_SLIDINGWINDOW);

        DEFAULT_APPCONFIG.put(AppConfig.CONFIGFETCHER_FETCHERINTERVAL, DefaultAppConfig.DEFAULT_CONFIGFETCHER_FETCHERINTERVAL);
    }
}
