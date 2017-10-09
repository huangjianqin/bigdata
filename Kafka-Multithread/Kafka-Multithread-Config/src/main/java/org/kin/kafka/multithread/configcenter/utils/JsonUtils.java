package org.kin.kafka.multithread.configcenter.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class JsonUtils {
    public static Map<String, String> json2Map(JSONObject json){
        Map<String, String> result = new HashMap<>();
        for(Map.Entry<String, Object> entry: json.entrySet()){
            result.put(entry.getKey(), entry.getValue().toString());
        }
        return result;
    }

    public static String map2Json(Map<String, String> map){
        JSONObject json = new JSONObject();
        for(Map.Entry<String, String> entry: map.entrySet()){
            json.put(entry.getKey(), entry.getValue());
        }
        return json.toJSONString();
    }
}
