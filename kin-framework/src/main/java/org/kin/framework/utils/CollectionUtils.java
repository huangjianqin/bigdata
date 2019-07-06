package org.kin.framework.utils;

import java.util.Collection;
import java.util.Map;

/**
 * @author huangjianqin
 * @date 2019/7/6
 */
public class CollectionUtils {
    public static <E> boolean isEmpty(Collection<E> collection){
        return collection == null || collection.isEmpty();
    }

    public static <E> boolean isNonEmpty(Collection<E> collection){
        return !isEmpty(collection);
    }

    public static <E> boolean isEmpty(E[] array){
        return array == null || array.length <= 0;
    }

    public static <E> boolean isNonEmpty(E[] array){
        return !isEmpty(array);
    }

    public static <K, V> boolean isEmpty(Map<K, V> map){
        return map == null || map.isEmpty();
    }

    public static <K, V> boolean isNonEmpty(Map<K, V> map){
        return !isEmpty(map);
    }
}
