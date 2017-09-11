package org.kin.kafka.multithread.configcenter.config;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class AppConfig {
    public static final String APPNAME = "appName";
    public static final String APPHOST = "appHost";
    public static final String APPSTATUS = "appStatus";

    public static final String MESSAGEFETCHER_POLL_TIMEOUT = "messagefetcher.poll.timeout";
    public static final String MESSAGEFETCHER_COMMIT_MAXRETRY = "messagefetcher.commit.maxretry";
    public static final String MESSAGEFETCHER_CONSUME_CALLBACK = "messagefetcher.consume.callback";

    public static final String MESSAGEHANDLER_MODEL = "messagehandler.model";

}
