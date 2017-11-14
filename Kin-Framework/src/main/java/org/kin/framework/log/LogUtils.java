package org.kin.framework.log;

/**
 * Created by huangjianqin on 2017/11/14.
 */
public class LogUtils {
    public static void debug(LogEvent logEvent){
        LoggerFactory.getAsyncFileLogger(logEvent).debug(logEvent.message());
    }

    public static void info(LogEvent logEvent){
        LoggerFactory.getAsyncFileLogger(logEvent).info(logEvent.message());
    }

    public static void warn(LogEvent logEvent){
        LoggerFactory.getAsyncFileLogger(logEvent).warn(logEvent.message());
    }

    public static void error(LogEvent logEvent){
        LoggerFactory.getAsyncFileLogger(logEvent).error(logEvent.message());
    }
}
