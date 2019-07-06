package org.kin.framework.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by huangjianqin on 2018/1/28.
 */
public class NetUtils {
    public static String localhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            ExceptionUtils.log(e);
        }
        return "127.0.0.1";
    }

    public static boolean checkHostPort(String address){
        return address.matches("\\S+:\\d{1,5}");
    }
}
