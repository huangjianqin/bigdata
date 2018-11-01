package org.kin.framework.utils;

/**
 * Created by huangjianqin on 2018/5/25.
 */
public class StringUtils {
    public static boolean isBlank(String s) {
        return s == null || s.equals("");
    }

    public static boolean isNotBlank(String s) {
        return !isBlank(s);
    }

    public static String reverse(String s) {
        if (isNotBlank(s)) {
            return new StringBuilder(s).reverse().toString();
        }

        return s;
    }
}
