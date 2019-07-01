package org.kin.framework.log;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import org.slf4j.Logger;

import java.io.File;

/**
 * Created by huangjianqin on 2017/11/14.
 */
public class LoggerFactory {
    public static final String BASE_PATH = "logs";

    public static Logger getAsyncFileLogger(AbstractLogEvent logEvent) {
        Logger logger = org.slf4j.LoggerFactory.getLogger(logEvent.getLoggerName());
        if (!(logger instanceof ch.qos.logback.classic.Logger)) {
            return logger;
        }

        ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
        if (logbackLogger.getAppender(logEvent.getAsyncAppenderName()) != null) {
            return logger;
        }
        //所有组件都必须start
        LoggerContext lc = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();

        ThresholdFilter infoFilter = new ThresholdFilter();
        infoFilter.setLevel("INFO");
        infoFilter.setContext(lc);
        infoFilter.start();

        TimeBasedRollingPolicy policy = new TimeBasedRollingPolicy();
        policy.setFileNamePattern(BASE_PATH + File.separator + "%d{yyyy-MM-dd}" + File.separator + logEvent.getFileName() + ".log.%d{yyyy-MM-dd}");
//        policy.setMaxHistory(30);
        policy.setContext(lc);

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(lc);
        encoder.setPattern("[%p] %d{yyyy-MM-dd HH:mm:ss SSS} [%t] |  %C.%M\\(%L\\) : %msg%n%ex");
        encoder.start();

        RollingFileAppender<ILoggingEvent> dailyRollingFileAppender = new RollingFileAppender<>();
        dailyRollingFileAppender.setContext(lc);
        dailyRollingFileAppender.setName(logEvent.getAppenderName());
        dailyRollingFileAppender.addFilter(infoFilter);
        dailyRollingFileAppender.setRollingPolicy(policy);
        dailyRollingFileAppender.setEncoder(encoder);
        dailyRollingFileAppender.setAppend(true);

        //下面三行很关键
        policy.setParent(dailyRollingFileAppender);
        policy.start();
        dailyRollingFileAppender.start();

        AsyncAppender asyncAppender = new AsyncAppender();
        asyncAppender.setContext(lc);
        asyncAppender.setName(logEvent.getAsyncAppenderName());
        asyncAppender.addAppender(dailyRollingFileAppender);
        //包含调用者信息
        asyncAppender.setIncludeCallerData(true);
        asyncAppender.start();

        logbackLogger.addAppender(asyncAppender);
        logbackLogger.setLevel(Level.INFO);
        logbackLogger.setAdditive(false);

        return logger;
    }
}
