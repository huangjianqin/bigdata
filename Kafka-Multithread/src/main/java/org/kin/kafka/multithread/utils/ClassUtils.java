package org.kin.kafka.multithread.utils;

import java.lang.reflect.Field;

/**
 * Created by 健勤 on 2017/7/21.
 */
public class ClassUtils {
    /**
     * 通过无参构造器实例化类
     * @param claxx
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> T instance(Class<T> claxx){
        if(claxx == null){
            return null;
        }
        try {
            return claxx.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Object instance(String classStr){
        if(classStr == null){
            return null;
        }
        try {
            Class claxx = Class.forName(classStr);
            return claxx.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据参数调用构造器实例化类
     * @param claxx
     * @param args
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> T instance(Class<T> claxx, Object... args){
        if(claxx == null){
            return null;
        }
        try {
            T target =  claxx.newInstance();
            for(Object o: args){
                Class oc = o.getClass();
                Class tmp = claxx;
                outer:
                while(tmp != null){
                    for(Field field: tmp.getDeclaredFields()){
                        if(field.getType().isAssignableFrom(oc)){
                            field.setAccessible(true);
                            field.set(target, o);
                            break outer;
                        }
                    }
                    tmp = tmp.getSuperclass();
                }
            }
            return target;
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
