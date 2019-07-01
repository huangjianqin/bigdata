package org.kin.hadoop.common.writable;

import org.apache.hadoop.io.WritableComparable;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by huangjianqin on 2017/9/4.
 * 写入顺序size(int), [keyBytes, valueBytes].......
 * <p>
 * 不能写入null
 * equal hashcode是对比实例引用
 * compareTo以key类型和key实例为基准
 * <p>
 * 本质上是基类
 * Comparator实现需先根据map长度判断,再对比key
 */
public class MapWritable<K extends WritableComparable, V extends WritableComparable> implements WritableComparable<MapWritable<K, V>>, Map<K, V> {
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private Map<K, V> map = new HashMap<>();

    public MapWritable(Class<K> keyClass, Class<V> valueClass) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }

    public MapWritable(Class<K> keyClass,
                       Class<V> valueClass,
                       Map<K, V> map,
                       boolean isOverwrite) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        if (isOverwrite) {
            this.map = map;
        } else {
            putAll(map);
        }
    }

    @Override
    public int compareTo(MapWritable o) {
        if (o == null) {
            return 1;
        }

        Integer thisSize = map.size();
        Integer thatsize = o.map.size();
        Integer lCmd = thisSize.compareTo(thatsize);
        if (lCmd != 0) {
            return lCmd;
        }

        Iterator<K> thisIterator = map.keySet().iterator();
        Iterator<K> thatIterator = o.map.keySet().iterator();
        while (thisIterator.hasNext() && thatIterator.hasNext()) {
            K thisWritableComparable = thisIterator.next();
            K thatWritableComparable = thatIterator.next();
            Integer cmd = thisWritableComparable.compareTo(thatWritableComparable);
            if (cmd != 0) {
                return cmd;
            }
        }


        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(map.size());
        for (Entry<K, V> entry : map.entrySet()) {
            entry.getKey().write(dataOutput);
            entry.getValue().write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            K key = ClassUtils.instance(keyClass);
            V value = ClassUtils.instance(valueClass);
            key.readFields(dataInput);
            value.readFields(dataInput);
            map.put(key, value);
        }
    }


    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return (V) map.get(key);
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        if (map.containsKey(key)) {
            return (V) map.get(key);
        }
        return defaultValue;
    }

    @Override
    public V put(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("key or value can't be null");
        }

        return (V) map.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if (!map.containsKey(key)) {
            map.put(key, value);
            return value;
        }
        return null;
    }

    @Override
    public V remove(Object key) {
        if (keyClass.isAssignableFrom(key.getClass())) {
            return (V) map.remove(key);
        }

        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        if (map.containsKey(null) || map.containsValue(null)) {
            throw new IllegalArgumentException("key or value can't be null");
        }
        this.map.putAll(map);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<K> keySet() {
        return (Set<K>) map.keySet();
    }

    @Override
    public Collection<V> values() {
        return (Collection<V>) map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> result = new HashSet<>();
        for (Entry<K, V> entry : map.entrySet()) {
            result.add((Entry<K, V>) entry);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MapWritable)) {
            return false;
        }

        MapWritable<?, ?> that = (MapWritable<?, ?>) o;

        if (keyClass != null ? !keyClass.equals(that.keyClass) : that.keyClass != null) {
            return false;
        }
        if (valueClass != null ? !valueClass.equals(that.valueClass) : that.valueClass != null) {
            return false;
        }
        return map != null ? map.equals(that.map) : that.map == null;
    }

    @Override
    public int hashCode() {
        int result = keyClass != null ? keyClass.hashCode() : 0;
        result = 31 * result + (valueClass != null ? valueClass.hashCode() : 0);
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return StringUtils.mkString(this);
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }
}
