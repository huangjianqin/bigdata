package org.kin.kafka.multithread.config;

import java.util.*;

/**
 * Created by 健勤 on 2017/7/21.
 * kafka多线程工具配置类
 * 与Kafka-Multithread-config AppConfig相同
 *
 * 此处忽略kafka producer和consumer配置,因为启动对应实例时会自动识别参数是否满足
 */
public class AppConfig {
    public static final Properties DEFAULT_APPCONFIG = new Properties();
    public static final List<String> REQUIRE_APPCONFIGS = new ArrayList<>();
    public static final Map<String, String> CONFIG2FORMATOR = new HashMap<>();
    public static final List<String> CAN_RECONFIG_APPCONFIGS = new ArrayList<>();

    //common
    //require
    public static final String APPNAME = "appName";
    /**
     * app所在节点,默认就是localhost
     * require
     */
    public static final String APPHOST = "appHost";
    /**
     * RUN
     * UPDATE
     * CLOSE
     * RESTART
     */
    //一次有效,仅仅是标识配置目的
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
    //定义KafkaConsumer poll(), ocot也会用到
    public static final String MESSAGEFETCHER_POLL_TIMEOUT = "messagefetcher.poll.timeout";
    public static final String MESSAGEFETCHER_COMMIT_ENABLERETRY = "messagefetcher.commit.enableretry";
    public static final String MESSAGEFETCHER_COMMIT_MAXRETRY = "messagefetcher.commit.maxretry";
    //仅支持RESTART和RUN状态
    public static final String MESSAGEFETCHER_CONSUME_CALLBACK = "messagefetcher.consume.callback";

    //message handler
    //require
    public static final String MESSAGEHANDLERMANAGER_MODEL = "messagehandlermanager.model";
    //仅支持RESTART和RUN状态
    //require
    public static final String MESSAGEHANDLER = "messagehandler";
    //只对OPOT和OCOT有效
    public static final String COMMITSTRATEGY = "commitstrategy";
    //end
    //对OPOT,OPMT,OPMT2无效
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

    //container
    /**
     * jvm,与app同一jvm
     * node,同一节点,不同jvm
     */
    public static final String APP_CHILD_RUN_MODEL = "app.child.run.model";

    static {
        DEFAULT_APPCONFIG.put(AppConfig.APPNAME, DefaultAppConfig.DEFAULT_NULL);
        DEFAULT_APPCONFIG.put(AppConfig.APPHOST, DefaultAppConfig.DEFAULT_APPHOST);
        DEFAULT_APPCONFIG.put(AppConfig.APPSTATUS, DefaultAppConfig.DEFAULT_APPSTATUS);

        DEFAULT_APPCONFIG.put(AppConfig.KAFKA_CONSUMER_SUBSCRIBE, DefaultAppConfig.DEFAULT_NULL);

        DEFAULT_APPCONFIG.put(MESSAGEFETCHER_POLL_TIMEOUT, DefaultAppConfig.DEFAULT_MESSAGEFETCHER_POLL_TIMEOUT);
        DEFAULT_APPCONFIG.put(MESSAGEFETCHER_COMMIT_ENABLERETRY, DefaultAppConfig.DEFAULT_MESSAGEFETCHER_COMMIT_ENABLERETRY);
        DEFAULT_APPCONFIG.put(MESSAGEFETCHER_COMMIT_MAXRETRY, DefaultAppConfig.DEFAULT_MESSAGEFETCHER_COMMIT_MAXRETRY);
        DEFAULT_APPCONFIG.put(MESSAGEFETCHER_CONSUME_CALLBACK, DefaultAppConfig.DEFAULT_NULL);

        DEFAULT_APPCONFIG.put(MESSAGEHANDLERMANAGER_MODEL, DefaultAppConfig.DEFAULT_NULL);
        DEFAULT_APPCONFIG.put(MESSAGEHANDLER, DefaultAppConfig.DEFAULT_NULL);
        DEFAULT_APPCONFIG.put(COMMITSTRATEGY, DefaultAppConfig.DEFAULT_NULL);
        DEFAULT_APPCONFIG.put(CONSUMERREBALANCELISTENER, DefaultAppConfig.DEFAULT_NULL);

        DEFAULT_APPCONFIG.put(OPMT_MAXTHREADSIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT_MAXTHREADSIZEPERPARTITION);
        DEFAULT_APPCONFIG.put(OPMT_MINTHREADSIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT_MINTHREADSIZEPERPARTITION);
        DEFAULT_APPCONFIG.put(OPMT_THREADQUEUESIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT_THREADQUEUESIZEPERPARTITION);
        DEFAULT_APPCONFIG.put(OPMT_HANDLERSIZE, DefaultAppConfig.DEFAULT_OPMT_HANDLERSIZE);

        DEFAULT_APPCONFIG.put(OPMT2_THREADSIZEPERPARTITION, DefaultAppConfig.DEFAULT_OPMT2_THREADSIZEPERPARTITION);

        DEFAULT_APPCONFIG.put(OCOT_CONSUMERNUM, DefaultAppConfig.DEFAULT_OCOT_CONSUMERNUM);

        DEFAULT_APPCONFIG.put(PENDINGWINDOW_SLIDINGWINDOW, DefaultAppConfig.DEFAULT_PENDINGWINDOW_SLIDINGWINDOW);

        DEFAULT_APPCONFIG.put(APP_CHILD_RUN_MODEL, DefaultAppConfig.DEFAULT_APP_CHILD_RUN_MODEL);
    }

    static {
        REQUIRE_APPCONFIGS.add(APPNAME);
        REQUIRE_APPCONFIGS.add(APPHOST);
        REQUIRE_APPCONFIGS.add(APPSTATUS);
        REQUIRE_APPCONFIGS.add(MESSAGEHANDLERMANAGER_MODEL);
        REQUIRE_APPCONFIGS.add(KAFKA_CONSUMER_SUBSCRIBE);
        REQUIRE_APPCONFIGS.add(MESSAGEHANDLER);
        REQUIRE_APPCONFIGS.add(COMMITSTRATEGY);
    }

    static {
        CONFIG2FORMATOR.put(AppConfig.APPNAME, ".*");
        CONFIG2FORMATOR.put(AppConfig.APPHOST, "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        CONFIG2FORMATOR.put(AppConfig.APPSTATUS, "(RUN)|(UPDATE)|(CLOSE)|(RESTART)");

        CONFIG2FORMATOR.put(AppConfig.KAFKA_CONSUMER_SUBSCRIBE, ".*");

        CONFIG2FORMATOR.put(MESSAGEFETCHER_POLL_TIMEOUT, "\\d*");
        CONFIG2FORMATOR.put(MESSAGEFETCHER_COMMIT_ENABLERETRY, "(true)|(false)");
        CONFIG2FORMATOR.put(MESSAGEFETCHER_COMMIT_MAXRETRY, "\\d*");
        CONFIG2FORMATOR.put(MESSAGEFETCHER_CONSUME_CALLBACK, ".*");

        CONFIG2FORMATOR.put(MESSAGEHANDLERMANAGER_MODEL, "(OPOT)|(OPMT)|(OPMT2)|(OTOC)");
        CONFIG2FORMATOR.put(MESSAGEHANDLER, ".*");
        CONFIG2FORMATOR.put(COMMITSTRATEGY, ".*");
        CONFIG2FORMATOR.put(CONSUMERREBALANCELISTENER, ".*");

        CONFIG2FORMATOR.put(OPMT_MAXTHREADSIZEPERPARTITION, "\\d+");
        CONFIG2FORMATOR.put(OPMT_MINTHREADSIZEPERPARTITION, "\\d+");
        CONFIG2FORMATOR.put(OPMT_THREADQUEUESIZEPERPARTITION, "\\d+");
        CONFIG2FORMATOR.put(OPMT_HANDLERSIZE, "\\d+");

        CONFIG2FORMATOR.put(OPMT2_THREADSIZEPERPARTITION, "\\d+");

        CONFIG2FORMATOR.put(OCOT_CONSUMERNUM, "\\d+");

        CONFIG2FORMATOR.put(PENDINGWINDOW_SLIDINGWINDOW, "\\d+");

    }

    static {
        CAN_RECONFIG_APPCONFIGS.add(MESSAGEFETCHER_POLL_TIMEOUT);
        CAN_RECONFIG_APPCONFIGS.add(MESSAGEFETCHER_COMMIT_ENABLERETRY);
        CAN_RECONFIG_APPCONFIGS.add(MESSAGEFETCHER_COMMIT_MAXRETRY);

        CAN_RECONFIG_APPCONFIGS.add(MESSAGEHANDLERMANAGER_MODEL);

        CAN_RECONFIG_APPCONFIGS.add(OPMT_MINTHREADSIZEPERPARTITION);
        CAN_RECONFIG_APPCONFIGS.add(OPMT_MAXTHREADSIZEPERPARTITION);
        CAN_RECONFIG_APPCONFIGS.add(OPMT_THREADQUEUESIZEPERPARTITION);
        CAN_RECONFIG_APPCONFIGS.add(OPMT_HANDLERSIZE);

        CAN_RECONFIG_APPCONFIGS.add(OPMT2_THREADSIZEPERPARTITION);

        CAN_RECONFIG_APPCONFIGS.add(OCOT_CONSUMERNUM);

        CAN_RECONFIG_APPCONFIGS.add(PENDINGWINDOW_SLIDINGWINDOW);
    }
}
