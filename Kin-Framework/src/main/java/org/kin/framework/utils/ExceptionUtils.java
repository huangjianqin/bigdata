package org.kin.framework.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangjianqin on 2017/10/28.
 */
public class ExceptionUtils {
    private static Logger log = LoggerFactory.getLogger("Error");

    public static void log(Throwable throwable){
        log.error("", throwable);
    }

    public static void log(String msg, Object... params){
        log.error(msg, params);
    }
}
