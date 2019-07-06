package org.kin.framework.utils;

import com.google.common.collect.ImmutableMap;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * @author huangjianqin
 * @date 2019/7/6
 */
public class YamlUtils {
    public static YamlConfig loadYamlL(String configPath) {
        try {
            //返回多层嵌套map
            return new YamlConfig((Map<String, Object>) Yaml.load(new File(configPath)));
        } catch (FileNotFoundException e) {
            ExceptionUtils.log(e);
        }
        return YamlConfig.empty();
    }

    public static Properties loadYaml2Properties(String configPath) {
        return loadYamlL(configPath).toProperties();
    }

    public static String transfer2YamlStr(Properties config) {
        return Yaml.dump(transfer2Yaml(config));
    }

    //------------------------------------------------------------------------------------------------------------------
    public static Properties transfer2Properties(Map<String, Object> yaml) {
        Properties properties = new Properties();
        transfer2Properties(yaml, properties, "");
        return properties;
    }

    /**
     * 将多层嵌套map转换成A.B.C的properties格式
     */
    private static void transfer2Properties(Map<String, Object> yaml, Properties properties, String keyHead) {
        for (String key : yaml.keySet()) {
            Object value = yaml.get(key);

            String propertiesKey = StringUtils.isBlank(keyHead) ? key : keyHead + "." + key;
            if (value instanceof Map) {
                transfer2Properties((Map<String, Object>) value, properties, propertiesKey);
            } else {
                if (Objects.isNull(value)) {
                    properties.put(propertiesKey, "");
                } else {
                    properties.put(propertiesKey, value);
                }
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    public static Map<String, Object> transfer2Yaml(Map config) {
        Map<String, Object> yaml = new HashMap<>();
        transfer2Yaml(yaml, config);
        return yaml;
    }

    /**
     * 将A.B.C的properties的map格式转换多层嵌套map
     */
    private static void transfer2Yaml(Map<String, Object> yaml, Map config) {
        for (Object key : config.keySet()) {
            String keyStr = key.toString();
            if (keyStr.contains("\\.")) {
                String[] split = keyStr.split("\\.", 2);
                Map<String, Object> nextLevel = yaml.containsKey(split[0]) ? (Map<String, Object>) yaml.get(split[0]) : new HashMap<>();
                yaml.put(split[0], nextLevel);
                deepMap(nextLevel, split[1], config.get(key));
            } else {
                yaml.put(key.toString(), config.get(key));
            }
        }
    }

    /**
     * 不断递归创建多层嵌套map
     * 尾递归,提交性能
     *
     * @param key 下面层数的key+.组成
     */
    private static void deepMap(Map<String, Object> nowLevel, String key, Object value) {
        if (key.contains("\\.")) {
            String[] split = key.split("\\.", 2);
            Map<String, Object> nextLevel = nowLevel.containsKey(split[0]) ? (Map<String, Object>) nowLevel.get(split[0]) : new HashMap<>();
            if (!nowLevel.containsKey(split[0])) {
                nowLevel.put(split[0], nextLevel);
            }
            deepMap(nextLevel, split[1], value);
        } else {
            nowLevel.put(key, value);
        }
    }


    public static class YamlConfig {
        public static final YamlConfig EMPTY = new YamlConfig(Collections.emptyMap());

        public static YamlConfig empty() {
            return EMPTY;
        }

        /**
         * key -> string, value -> object
         */
        private Map<String, Object> yaml;

        public YamlConfig(Map<String, Object> yml) {
            this.yaml = ImmutableMap.copyOf(yml);
        }

        public YamlConfig(Properties properties) {
            this.yaml = transfer2Yaml(properties);
        }

        public Properties toProperties() {
            return transfer2Properties(yaml);
        }

        public String toYamlStr() {
            return Yaml.dump(yaml);
        }

        /**
         * @param key XX.YY.ZZ.....
         * @return value
         */
        public Object get(String key) {
            Map copy = new HashMap<>(yaml);
            String[] splitKeys = key.split("\\.");
            for (int i = 0; i < splitKeys.length; i++) {
                String splitKey = splitKeys[i];
                Object tmpValue = copy.get(splitKey);
                if (!(tmpValue instanceof Map)) {
                    if (i == splitKeys.length - 1) {
                        return tmpValue;
                    } else {
                        //异常
                        return null;
                    }
                }

                copy = new HashMap<>((Map) tmpValue);

            }

            return null;
        }
    }
}
