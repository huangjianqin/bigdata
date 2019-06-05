package org.kin.framework.log;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/11/4.
 * log4j api动态绑定logger
 */
public class Log4jLoggerBinder {
    private Properties properties = new Properties();

    //example: [INFO] 2017-02-09 22:54:30 774 [main] | LogTest.main(10) : testing
    public static String DEFAULT_PATTERN = "[%p] %d{yyyy-MM-dd HH\\:mm\\:ss SSS} [%t] | %C.%M(%L) : %m %n";
    public static String DEFAULT_DATEPATTERN = "'.'yyyy-MM-dd";

    public static String DEFAULT_CONSOLE_APPENDER_NAME = "console";
    public static String DEFAULT_STDOUT_APPENDER_NAME = "stdout";
    public static String DEFAULT_ERROR_APPENDER_NAME = "error";

    //common
    public static String ROOT_CATEGORY = "log4j.rootCategory";
    public static String ROOT_LOGGER = "log4j.rootLogger";
    public static String APPENDER = "log4j.appender.%s";
    public static String LOGGER = "log4j.logger.%s";

    //layout
    public static String LAYOUT = "log4j.appender.%s.layout";
    public static String LAYOUT_CONVERSIONPATTERN = "log4j.appender.%s.layout.ConversionPattern";

    //file log
    public static String FILE = "log4j.appender.%s.File";
    public static String APPEND = "log4j.appender.%s.Append";
    public static String THRESHOLD = "log4j.appender.%s.Threshold";
    public static String DATEPATTERN = "log4j.appender.%s.DatePattern";

    //Appender
    public static String CONSOLE_APPENDER = "org.apache.log4j.ConsoleAppender";
    public static String DAILY_ROLLING_FILE_APPENDER = "org.apache.log4j.DailyRollingFileAppender";
    public static String FILE_APPENDER = "org.apache.log4j.FileAppender";
    public static String ROLLING_FILE_APPENDER = "org.apache.log4j.RollingFileAppender";
    public static String WRITER_APPENDER = "org.apache.log4j.WriterAppender";

    //Layout
    public static String PATTERN_LAYOUT = "org.apache.log4j.PatternLayout";
    public static String HTML_LAYOUT = "org.apache.log4j.HTMLLayout";
    public static String SIMPLE_LAYOUT = "org.apache.log4j.SimpleLayout";
    public static String TTCC_LAYOUT = "org.apache.log4j.TTCCLayout";

    public static Boolean exist(String logger) {
        return LogManager.exists(logger) != null;
    }

    public static Log4jLoggerBinder create() {
        return new Log4jLoggerBinder();
    }

    public void bind() {
        PropertyConfigurator.configure(properties);
    }

    public Log4jLoggerBinder setRootCategory(Level level, String... appenders) {
        String value = level.toString();
        for (String appender : appenders) {
            value += "," + appender;
        }

        properties.setProperty(ROOT_CATEGORY, value);
        return this;
    }

    public Log4jLoggerBinder setRootLogger(Level level, String... appenders) {
        String value = level.toString();
        for (String appender : appenders) {
            value += "," + appender;
        }

        properties.setProperty(ROOT_LOGGER, value);
        return this;
    }

    public Log4jLoggerBinder setLogger(Level level, String logger, String... appenders) {
        String value = level.toString();
        for (String appender : appenders) {
            value += "," + appender;
        }

        properties.setProperty(String.format(LOGGER, logger), value);
        return this;
    }

    public Log4jLoggerBinder setDailyRollingFileAppender(String appender) {
        properties.setProperty(String.format(APPENDER, appender), DAILY_ROLLING_FILE_APPENDER);
        return this;
    }

    public Log4jLoggerBinder setFileAppender(String appender) {
        properties.setProperty(String.format(APPENDER, appender), FILE_APPENDER);
        return this;
    }

    public Log4jLoggerBinder setRollingFileAppender(String appender) {
        properties.setProperty(String.format(APPENDER, appender), ROLLING_FILE_APPENDER);
        return this;
    }

    public Log4jLoggerBinder setConsoleAppender(String appender) {
        properties.setProperty(String.format(APPENDER, appender), CONSOLE_APPENDER);
        return this;
    }

    public Log4jLoggerBinder setWriterAppender(String appender) {
        properties.setProperty(String.format(APPENDER, appender), WRITER_APPENDER);
        return this;
    }

    public Log4jLoggerBinder setAppender(String appender, String appenderClass) {
        properties.setProperty(String.format(APPENDER, appender), appenderClass);
        return this;
    }

    public Log4jLoggerBinder setAppender(String appender, Class appenderClass) {
        properties.setProperty(String.format(APPENDER, appender), appenderClass.getName());
        return this;
    }

    public Log4jLoggerBinder setPatternLayout(String appender) {
        properties.setProperty(String.format(LAYOUT, appender), PATTERN_LAYOUT);
        return this;
    }

    public Log4jLoggerBinder setHtmlLayout(String appender) {
        properties.setProperty(String.format(LAYOUT, appender), HTML_LAYOUT);
        return this;
    }

    public Log4jLoggerBinder setSimpleLayout(String appender) {
        properties.setProperty(String.format(LAYOUT, appender), SIMPLE_LAYOUT);
        return this;
    }

    public Log4jLoggerBinder setTTCCLayout(String appender) {
        properties.setProperty(String.format(LAYOUT, appender), TTCC_LAYOUT);
        return this;
    }

    public Log4jLoggerBinder setPatternLayout(String appender, String layoutClass) {
        properties.setProperty(String.format(LAYOUT, appender), layoutClass);
        return this;
    }

    public Log4jLoggerBinder setPatternLayout(String appender, Class layoutClass) {
        properties.setProperty(String.format(LAYOUT, appender), layoutClass.getName());
        return this;
    }

    public Log4jLoggerBinder setConversionPattern(String appender) {
        properties.setProperty(String.format(LAYOUT_CONVERSIONPATTERN, appender), DEFAULT_PATTERN);
        return this;
    }

    public Log4jLoggerBinder setConversionPattern(String appender, String pattern) {
        properties.setProperty(String.format(LAYOUT_CONVERSIONPATTERN, appender), pattern);
        return this;
    }

    public Log4jLoggerBinder setFile(String appender, String path) {
        properties.setProperty(String.format(FILE, appender), path);
        return this;
    }

    public Log4jLoggerBinder setDatePattern(String appender) {
        properties.setProperty(String.format(DATEPATTERN, appender), DEFAULT_DATEPATTERN);
        return this;
    }

    public Log4jLoggerBinder setDatePattern(String appender, String pattern) {
        properties.setProperty(String.format(DATEPATTERN, appender), pattern);
        return this;
    }

    public Log4jLoggerBinder setThreshold(String appender, String level) {
        properties.setProperty(String.format(THRESHOLD, appender), level);
        return this;
    }

    public Log4jLoggerBinder setThreshold(String appender, Level level) {
        properties.setProperty(String.format(THRESHOLD, appender), level.toString());
        return this;
    }

    public Log4jLoggerBinder setAppend(String appender, String value) {
        properties.setProperty(String.format(APPEND, appender), Boolean.valueOf(value).toString());
        return this;
    }

    public Log4jLoggerBinder setAppend(String appender, Boolean value) {
        properties.setProperty(String.format(APPEND, appender), value.toString());
        return this;
    }

    public Log4jLoggerBinder addConsoleAppender() {
        return addConsoleAppender(DEFAULT_CONSOLE_APPENDER_NAME);
    }

    public Log4jLoggerBinder addConsoleAppender(String appender) {
        return setConsoleAppender(appender)
                .setPatternLayout(appender)
                .setConversionPattern(appender);
    }

    public Log4jLoggerBinder addStdoutAppender(String path) {
        return addStdoutAppender(DEFAULT_STDOUT_APPENDER_NAME, path);
    }

    public Log4jLoggerBinder addStdoutAppender(String appender, String path) {
        return setDailyRollingFileAppender(appender)
                .setFile(appender, path)
                .setDatePattern(appender)
                .setThreshold(appender, Level.INFO)
                .setAppend(appender, true)
                .setPatternLayout(appender)
                .setConversionPattern(appender);
    }

    public Log4jLoggerBinder addErrorAppender(String path) {
        return addStdoutAppender(DEFAULT_ERROR_APPENDER_NAME, path);
    }

    public Log4jLoggerBinder addErrorAppender(String appender, String path) {
        return setDailyRollingFileAppender(appender)
                .setFile(appender, path)
                .setDatePattern(appender)
                .setThreshold(appender, Level.ERROR)
                .setAppend(appender, true)
                .setPatternLayout(appender)
                .setConversionPattern(appender);
    }

}
