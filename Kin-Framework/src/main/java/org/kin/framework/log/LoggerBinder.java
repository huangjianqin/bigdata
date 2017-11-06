package org.kin.framework.log;

import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/11/4.
 */
public class LoggerBinder {
    private Properties properties = new Properties();

    public static String DEFAULT_PATTERN = "[%p] %d{yyyy-MM-dd HH\\:mm\\:ss SSS} [%t] | %C.%M(%L) : %m %n";
    public static String DEFAULT_DATEPATTERN = "'.'yyyy-MM-dd";

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
    public static String DAILY_FILE_APPENDER = "org.apache.log4j.DailyRollingFileAppender";

    //Layout
    public static String PATTERN_LAYOUT = "org.apache.log4j.PatternLayout";

    public static LoggerBinder create(){
        return new LoggerBinder();
    }

    public void bind(){
        PropertyConfigurator.configure(properties);
    }

    public LoggerBinder setRootCategory(Level level, String... appenders){
        String value = level.toString();
        for(String appender: appenders){
            value += "," + appender;
        }

        properties.setProperty(ROOT_CATEGORY, value);
        return this;
    }

    public LoggerBinder setRootLogger(Level level, String... appenders){
        String value = level.toString();
        for(String appender: appenders){
            value += "," + appender;
        }

        properties.setProperty(ROOT_LOGGER, value);
        return this;
    }

    public LoggerBinder setLogger(Level level, String logger, String... appenders){
        String value = level.toString();
        for(String appender: appenders){
            value += "," + appender;
        }

        properties.setProperty(String.format(LOGGER, logger), value);
        return this;
    }

    public LoggerBinder setDailyFileAppender(String appender){
        properties.setProperty(String.format(APPENDER, appender), DAILY_FILE_APPENDER);
        return this;
    }

    public LoggerBinder setConsoleAppender(String appender){
        properties.setProperty(String.format(APPENDER, appender), CONSOLE_APPENDER);
        return this;
    }

    public LoggerBinder setAppender(String appender, String appenderClass){
        properties.setProperty(String.format(APPENDER, appender), appenderClass);
        return this;
    }

    public LoggerBinder setAppender(String appender, Class appenderClass){
        properties.setProperty(String.format(APPENDER, appender), appenderClass.getName());
        return this;
    }

    public LoggerBinder setLayout(String appender){
        properties.setProperty(String.format(LAYOUT, appender), PATTERN_LAYOUT);
        return this;
    }

    public LoggerBinder setLayout(String appender, String layoutClass){
        properties.setProperty(String.format(LAYOUT, appender), layoutClass);
        return this;
    }

    public LoggerBinder setLayout(String appender, Class layoutClass){
        properties.setProperty(String.format(LAYOUT, appender), layoutClass.getName());
        return this;
    }

    public LoggerBinder setConversionPattern(String appender){
        properties.setProperty(String.format(LAYOUT_CONVERSIONPATTERN, appender), DEFAULT_PATTERN);
        return this;
    }

    public LoggerBinder setConversionPattern(String appender, String pattern){
        properties.setProperty(String.format(LAYOUT_CONVERSIONPATTERN, appender), pattern);
        return this;
    }

    public LoggerBinder setFile(String appender, String path){
        properties.setProperty(String.format(FILE, appender), path);
        return this;
    }

    public LoggerBinder setDatePattern(String appender){
        properties.setProperty(String.format(DATEPATTERN, appender), DEFAULT_DATEPATTERN);
        return this;
    }

    public LoggerBinder setDatePattern(String appender, String pattern){
        properties.setProperty(String.format(DATEPATTERN, appender), pattern);
        return this;
    }

    public LoggerBinder setThreshold(String appender, String value){
        properties.setProperty(String.format(THRESHOLD, appender), value);
        return this;
    }

    public LoggerBinder setAppend(String appender, String value){
        properties.setProperty(String.format(APPEND, appender), value);
        return this;
    }

}
