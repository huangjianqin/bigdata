package org.kin.framework.log;

/**
 * Created by huangjianqin on 2017/11/14.
 * 临时用于对某事件进行log,而不用特意修改配置文件
 */
public abstract class LogEvent {
    protected String fileName;
    protected String loggerName;
    protected String appenderName;

    public abstract String message();

    public String getFileName(){
        if(fileName == null || fileName.equals("")){
            String className = getClass().getSimpleName();
            int lastIndex = className.lastIndexOf("LogEvent");
            fileName = lastIndex == -1 ? className : className.substring(0, lastIndex);
        }
        return fileName;
    }

    public String getLoggerName(){
        if(loggerName == null || loggerName.equals("")){
            loggerName = getFileName() + "Logger";
        }
        return loggerName;
    }

    public String getAppenderName(){
        if(appenderName == null || appenderName.equals("")){
            appenderName = getLoggerName() + "Appender";
        }
        return appenderName;
    }

    public String getAsyncAppenderName(){
        if(appenderName == null || appenderName.equals("")){
            return "async" + getAppenderName();
        }
        return "async" + appenderName;
    }

    public void debug(){
        LogUtils.debug(this);
    }

    public void info(){
        LogUtils.info(this);
    }

    public void warn(){
        LogUtils.warn(this);
    }

    public void error(){
        LogUtils.error(this);
    }
}
