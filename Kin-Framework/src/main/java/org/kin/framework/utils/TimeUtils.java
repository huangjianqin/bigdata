package org.kin.framework.utils;

import java.util.concurrent.TimeUnit;

/**
 * Created by huangjianqin on 2018/2/2.
 */
public class TimeUtils {
    public static int timestamp() {
        return (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }
}
