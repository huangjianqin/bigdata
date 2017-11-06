package org.kin.kafka.multithread.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huangjianqin on 2017/10/27.
 */
public class StringUtils {
    public static String[] getHostAndAppName(String regex, String target){
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(target);
        if(matcher.matches()){
            return new String[]{matcher.group(1), matcher.group(2)};
        }
        return null;
    }
}
