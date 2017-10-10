package org.kin.kafka.multithread.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by huangjianqin on 2017/9/19.
 */
public class HostUtils {
    public static String localhost(){
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return "localhost";
    }

    public static void main(String[] args) {
        System.out.println(localhost());
    }
}
