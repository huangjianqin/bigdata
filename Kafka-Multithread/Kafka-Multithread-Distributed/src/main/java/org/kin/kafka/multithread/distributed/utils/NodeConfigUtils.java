package org.kin.kafka.multithread.distributed.utils;

import org.kin.kafka.multithread.distributed.node.config.NodeConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/20.
 */
public class NodeConfigUtils {
    public static void fillDefaultConfig(Properties config){
        if(config == null){
            return;
        }
        Properties tmp = deepCopy(NodeConfig.DEFAULT_NODECONFIG);
        tmp.putAll(config);
        config.clear();
        config.putAll(tmp);
    }

    public static Properties deepCopy(Properties config){
        if(config == null){
            return null;
        }

        Properties clonedProperties = new Properties();
        for(Map.Entry<Object, Object> entry: config.entrySet()){
            clonedProperties.put(new String(entry.getKey().toString()), new String(entry.getValue().toString()));
        }
        return clonedProperties;
    }

    public static boolean isConfigItemChange(Properties lastConfig, Properties newConfig, Object key){
        if(lastConfig == null && newConfig == null){
            throw new IllegalStateException("last properties or new properties state wrong");
        }

        if(lastConfig == null && newConfig != null){
            return true;
        }

        if(lastConfig != null && newConfig == null){
            return false;
        }

        if(lastConfig.containsKey(key) && newConfig.containsKey(key)){
            if(!lastConfig.get(key).equals(newConfig.get(key))){
                return true;
            }
            else{
                return false;
            }
        }
        else{
            throw new IllegalStateException("last properties or new properties state wrong");
        }
    }

    public static boolean isConfigItemChange(Object lastValue, Properties newConfig, Object key){
        if(newConfig.containsKey(key)){
            if(!lastValue.equals(newConfig.get(key))){
                return true;
            }
            else{
                return false;
            }
        }
        else{
            throw new IllegalStateException("new properties state wrong");
        }
    }

    public static List<Properties> allNecessaryCheckAndFill(List<Properties> newConfigs){
        List<Properties> result = new ArrayList<>();
        for(Properties config: newConfigs){
            oneNecessaryCheckAndFill(config);
            if(config != null){
                result.add(config);
            }
        }
        return result;
    }

    public static void oneNecessaryCheckAndFill(Properties newConfig){
        //填充默认值
        NodeConfigUtils.fillDefaultConfig(newConfig);
        //检查配置格式
        if(!checkConfigValueFormat(newConfig)){
            return;
        }
    }

    public static boolean checkConfigValueFormat(Properties config){
        for(Map.Entry<String, String> entry: NodeConfig.CONFIG2FORMATOR.entrySet()){
            //如果=默认值,则不管格式问题
            String value = config.getProperty(entry.getKey());
            if(!value.equals(NodeConfig.DEFAULT_NODECONFIG.get(entry.getKey())) &&
                    !config.getProperty(entry.getKey()).matches(value)){
                throw new IllegalStateException("config \"" +  entry.getKey() + "\" 's value \"" + entry.getValue() + "\" format is not correct");
            }
        }
        return true;
    }

    public static String toString(Properties config){
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Object, Object> entry: config.entrySet()){
            sb.append(entry.getKey() + "  =  " + entry.getValue() + System.lineSeparator());
        }
        return sb.toString();
    }
}
