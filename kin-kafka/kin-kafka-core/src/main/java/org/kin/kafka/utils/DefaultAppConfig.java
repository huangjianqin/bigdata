package org.kin.kafka.utils;

import org.kin.framework.utils.NetUtils;


/**
 * Created by 健勤 on 2017/7/21.
 * 各配置项对应的默认值
 */
public class DefaultAppConfig {
    public static final String DEFAULT_NULL = "";

    public static final String DEFAULT_APPHOST = NetUtils.getIp();

    public static final String OPOT = "OPOT";
    public static final String OPMT = "OPMT";
    public static final String OPMT2 = "OPMT2";
    public static final String OCOT = "OCOT";

    //message fetcher
    public static final String DEFAULT_MESSAGEFETCHER_POLL_TIMEOUT = "1000";
    public static final String DEFAULT_MESSAGEFETCHER_COMMIT_ENABLERETRY = "false";
    public static final String DEFAULT_MESSAGEFETCHER_COMMIT_MAXRETRY = "5";
    public static final String DEFAULT_MESSAGEFETCHER_CONSUME_CALLBACK = "";

    //message handler
    public static final String DEFAULT_MESSAGEHANDLER = "org.kin.kafka.multithread.api.impl.DefaultMessageHandler";
    public static final String DEFAULT_COMMITSTRATEGY = "org.kin.kafka.multithread.api.impl.DefaultCommitStrategy";
    //OPOT,OPMT,OPMT2默认使用内置的listener, OCOT默认就""
    public static final String DEFAULT_CONSUMERREBALANCELISTENER = "";

    //opot

    //opmt
    public static final String DEFAULT_OPMT_MAXTHREADSIZEPERPARTITION = (Runtime.getRuntime().availableProcessors() * 2 - 1) + "";
    public static final String DEFAULT_OPMT_MINTHREADSIZEPERPARTITION = "2";
    public static final String DEFAULT_OPMT_THREADQUEUESIZEPERPARTITION = 10000 * 10000 + "";
    public static final String DEFAULT_OPMT_HANDLERSIZE = "10";

    //opmt2
    public static final String DEFAULT_OPMT2_THREADSIZEPERPARTITION = (Runtime.getRuntime().availableProcessors() * 2 - 1) + "";

    //ocot
    public static final String DEFAULT_OCOT_CONSUMERNUM = "1";

    //pendingwindow
    public static final String DEFAULT_PENDINGWINDOW_SLIDINGWINDOW = "1000";

    //container
    /**
     * jvm,与app同一jvm
     * node
     */
    public static final String DEFAULT_APP_CHILD_RUN_MODEL = "jvm";
}
