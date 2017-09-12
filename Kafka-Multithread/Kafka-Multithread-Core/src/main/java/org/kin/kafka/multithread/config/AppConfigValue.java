package org.kin.kafka.multithread.config;

/**
 * Created by 健勤 on 2017/7/21.
 * 各配置项对应的默认值
 */
public class AppConfigValue {
    public static final String OPOT = "OPOT";
    public static final String OPMT = "OPMT";
    public static final String OPMT2 = "OPMT2";
    public static final String OCOT = "OCOT";

    //kafka consumer
    public static final String DEFAULT_KAFKA_CONSUMER_PAUSE = "";
    public static final String DEFAULT_KAFKA_CONSUMER_RESUME = "";
    public static final String DEFAULT_KAFKA_CONSUMER_SUBSCRIBE = "";
    //message fetcher
    public static final String DEFAULT_MESSAGEFETCHER_POLL_TIMEOUT = "";
    public static final String DEFAULT_MESSAGEFETCHER_COMMIT_MAXRETRY = "";
    public static final String DEFAULT_MESSAGEFETCHER_CONSUME_CALLBACK = "";

    //message handler
    public static final String DEFAULT_MESSAGEHANDLER_QUEUEPERTHREAD_POLLTIMEOUT = "";

    //opot

    //opmt
    public static final String DEFAULT_OPMT_THREADSIZEPERPARTITION = "";
    public static final String DEFAULT_OPMT_THREADQUEUESIZEPERPARTITION = "";
    public static final String DEFAULT_OPMT_HANDLERSIZE = "";

    //opmt2
    public static final String DEFAULT_OPMT2_THREADSIZEPERPARTITION = "";

    //ocot
    public static final String DEFAULT_OCOT_CONSUMERNUM = "";

    //pendingwindow
    public static final String DEFAULT_PENDINGWINDOW_SLIDINGWINDOW = "";

    //config fetcher
    public static final String DEFAULT_CONFIGFETCHER_FETCHERINTERVAL = "";

    //本地
    //配置中心节点信息
    public static final String DEFAULT_CONFIGCENTER_HOST = "";
    public static final String DEFAULT_CONFIGCENTER_PORT = "";

    //container
    /**
     * jvm,与app同一jvm
     * node
     */
    public static final String DEFAULT_APP_CHILD_RUN_MODEL = "app.child.run.model";
}
