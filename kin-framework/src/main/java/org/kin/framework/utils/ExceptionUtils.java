package org.kin.framework.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangjianqin on 2018/1/28.
 */
public class ExceptionUtils {
    private static Logger log = LoggerFactory.getLogger("error");

    public static void log(Throwable throwable) {
        log.error(throwable.getMessage(), throwable);
    }

    public static void log(Throwable throwable, String msg, Object... params) {
        log.error(String.format(msg, params), throwable);
    }

    public static void log(String msg, Object... params) {
        log.error(msg, params);
    }
}
