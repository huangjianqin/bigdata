package org.kin.framework.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2019/6/18.
 */
public class HttpUtils {
    public static Map<String, String> parseQuery(String query){
        if(StringUtils.isNotBlank(query)){
            String[] splits = query.split("&");
            Map<String, String> params = new HashMap<>(splits.length);
            for(String kv: splits){
                String[] kvArr = kv.split("=");
                if(kvArr.length == 2){
                    params.put(kvArr[0], kvArr[1]);
                }
                else{
                    params.put(kvArr[0], "");
                }
            }

            return params;
        }

        return Collections.emptyMap();
    }
}
