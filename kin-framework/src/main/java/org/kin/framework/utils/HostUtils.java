package org.kin.framework.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by huangjianqin on 2018/1/28.
 */
public class HostUtils {
    public static String localhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            ExceptionUtils.log(e);
        }
        return "127.0.0.1";
    }
}
