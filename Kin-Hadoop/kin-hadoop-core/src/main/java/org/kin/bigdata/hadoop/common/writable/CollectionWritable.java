package org.kin.bigdata.hadoop.common.writable;

import org.apache.hadoop.io.WritableComparable;
import org.kin.framework.utils.ClassUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by huangjianqin on 2017/9/4.
 * 写入顺序size(int),[itemBytes]........
 *
 * 不能写入null
 * equal hashcode是对比实例引用
 * compareTo以item类型和集合item为基准
 *
 * 本质上是基类
 * Comparator实现需先根据collection长度判断,再对比元素
 */
public class CollectionWritable<T extends WritableComparable> implements WritableComparable<CollectionWritable<T>>, Collection<T> {
    //作为成员进行读操作的容器
    private final Class<T> itemType;
    private Collection<T> collection = new ArrayList<>();

    public CollectionWritable(Class<T> itemType) {
        this.itemType = itemType;
    }


    public CollectionWritable(Class<T> itemType, Collection<T> collection, boolean isOverwrite){
        this(itemType);
        if(isOverwrite){
            this.collection = (Collection<T>) collection;
        }
        else{
            addAll(collection);
        }
    }

    public String mkString(String separator){
        if(separator == null || separator.equals("")){
            separator = ",";
        }

        StringBuilder sb = new StringBuilder();
        for(WritableComparable item: collection){
            sb.append(item.toString() + separator);
        }
        if(collection.size() > 0){
            sb.replace(sb.length() - separator.length(), sb.length(), "");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //写入集合数量
        dataOutput.writeInt(collection.size());
        for(WritableComparable item: collection){
            item.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for(int i = 0; i < size; i++){
            WritableComparable item = ClassUtils.instance(itemType);
            item.readFields(dataInput);
            collection.add((T) item);
        }
    }

    @Override
    public int compareTo(CollectionWritable o) {
        if(o == null){
            return 1;
        }

        WritableComparable[] thisArr = collection.toArray(new WritableComparable[1]);
        WritableComparable[] thatArr = (WritableComparable[]) o.collection.toArray(new WritableComparable[1]);

        Integer thisArrL = thisArr.length;
        Integer thatArrL = thatArr.length;
        Integer lCmd = thisArrL.compareTo(thatArrL);
        if(lCmd != 0){
            return lCmd;
        }

        for(int i = 0; i < thisArr.length; i++){
            int cmd = thisArr[i].compareTo(thatArr[i]);
            if(cmd == 1){
                return 1;
            }
            else if(cmd == -1){
                return -1;
            }
        }

        return 0;
    }

    @Override
    public int size() {
        return collection.size();
    }

    @Override
    public boolean isEmpty() {
        return collection.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return collection.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return collection.iterator();
    }

    @Override
    public Object[] toArray() {
        return collection.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return collection.toArray(a);
    }

    @Override
    public boolean add(T item) {
        if(item == null){
            throw new IllegalArgumentException("item can't be null");
        }

        return collection.add(item);
    }

    @Override
    public boolean remove(Object o) {
        //当使用sorted列表,集合时,防止传入不合法的Comparable实例
        if(o instanceof WritableComparable){
            return collection.remove(o);
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return collection.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        if(c.contains(null)){
            throw new IllegalArgumentException("item can't be null");
        }
        return collection.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        //当使用sorted列表,集合时,防止传入不合法的Comparable实例,比如自身CollectionWritable.....
        if(c instanceof CollectionWritable){
            for(Object o: c){
                if(!collection.remove(o)){
                    return false;
                }
            }
            return true;
        }
        return collection.remove(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return collection.retainAll(c);
    }

    @Override
    public void clear() {
        collection.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CollectionWritable)) return false;

        CollectionWritable<?> that = (CollectionWritable<?>) o;

        if (itemType != null ? !itemType.equals(that.itemType) : that.itemType != null) return false;
        return collection != null ? collection.equals(that.collection) : that.collection == null;
    }

    @Override
    public int hashCode() {
        int result = itemType != null ? itemType.hashCode() : 0;
        result = 31 * result + (collection != null ? collection.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return mkString(",");
    }

    public Class<? extends WritableComparable> getItemType() {
        return itemType;
    }

}
