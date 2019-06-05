package org.kin.framework.utils;

import java.util.Collection;

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

    public static String mkString(String separator, String... contents) {
        if (contents != null && contents.length > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(contents[0]);

            for (int i = 1; i < contents.length; i++) {
                sb.append(", " + contents[i]);
            }

            return sb.toString();
        }

        return null;
    }

    public static String mkString(String separator, Collection collection) {
        if (collection != null && collection.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for(Object o: collection){
                sb.append(o + ", ");
            }
            sb.replace(sb.length() - 1, sb.length(), "");

            return sb.toString();
        }

        return null;
    }
}
