package org.kin.bigdata.hadoop.common;

import org.apache.hadoop.io.Text;
import org.kin.bigdata.hadoop.common.writable.MapWritable;
import org.kin.bigdata.hadoop.common.writable.Text2TextMapWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/7.
 */
public class Text2TextMapWritableTest extends WritableTestBase{
    static MapWritable mr1;
    static MapWritable mr2;
    static MapWritable mr3;
    static MapWritable mr4;

    static Text2TextMapWritable.Text2TextMapComparator comparator = new Text2TextMapWritable.Text2TextMapComparator();

    static byte[] mrb1;
    static byte[] mrb2;
    static byte[] mrb3;
    static byte[] mrb4;

    static {
        Map<Text, Text> m1 = new HashMap<>();
        m1.put(new Text("1"), new Text("aaaa"));
        m1.put(new Text("2"), new Text("bbbb"));
        m1.put(new Text("3"), new Text("cccc"));
        m1.put(new Text("4"), new Text("eeee"));

        Map<Text, Text> m2 = new HashMap<>();
        m2.put(new Text("1"), new Text("aaaa"));
        m2.put(new Text("2"), new Text("bbbb"));
        m2.put(new Text("3"), new Text("cccc"));
        m2.put(new Text("4"), new Text("eeee"));

        Map<Text, Text> m3 = new HashMap<>();
        m3.put(new Text("1"), new Text("aaaa"));
        m3.put(new Text("2"), new Text("bbbb"));
        m3.put(new Text("3"), new Text("cccc"));

        Map<Text, Text> m4 = new HashMap<>();
        m4.put(new Text("1"), new Text("aaaa"));
        m4.put(new Text("2"), new Text("bbbb"));
        m4.put(new Text("3"), new Text("cccc"));
        m4.put(new Text("4"), new Text("eeee"));
        m4.put(new Text("5"), new Text("dddd"));

        mr1 = new Text2TextMapWritable(m1);
        mr2 = new Text2TextMapWritable(m2);
        mr3 = new Text2TextMapWritable(m3);
        mr4 = new Text2TextMapWritable(m4);

        try {
            mrb1 = serialize(mr1);
            mrb2 = serialize(mr2);
            mrb3 = serialize(mr3);
            mrb4 = serialize(mr4);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        testCommonOperation();
//        testEqualAndHashCode();
//        testComparator1();
        testComparator2();
    }

    public static void testCommonOperation() {
        mr1.putAll(mr4);
        System.out.println(mr1.mkString(","));
        for(Object key: mr2.keySet()){
            mr1.remove(key);
        }
        System.out.println(mr1.mkString(","));
        for(Object key: mr2.keySet()){
            System.out.println(mr1.get(key));
        }
        System.out.println("-------------------------------------");
        System.out.println(mr2.getOrDefault("5", new Text("eeee")));
        System.out.println(mr2.putIfAbsent(new Text("1"), new Text("aaaa1")));
        System.out.println(mr2.putIfAbsent(new Text("5"), new Text("eeee")));
        mr2.clear();
        System.out.println(mr2.mkString(","));
        System.out.println(mr2.isEmpty());
    }

    public static void testComparator2(){
        System.out.println(comparator.compare(mrb1, 0, mrb1.length, mrb1, 0, mrb1.length));
        System.out.println(comparator.compare(mrb1, 0, mrb1.length, mrb2, 0, mrb2.length));
        System.out.println(comparator.compare(mrb1, 0, mrb1.length, mrb3, 0, mrb3.length));
        System.out.println(comparator.compare(mrb1, 0, mrb1.length, mrb4, 0, mrb4.length));
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println(comparator.compare(mrb2, 0, mrb2.length, mrb1, 0, mrb1.length));
        System.out.println(comparator.compare(mrb2, 0, mrb2.length, mrb2, 0, mrb2.length));
        System.out.println(comparator.compare(mrb2, 0, mrb2.length, mrb3, 0, mrb3.length));
        System.out.println(comparator.compare(mrb2, 0, mrb2.length, mrb4, 0, mrb4.length));
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println(comparator.compare(mrb3, 0, mrb3.length, mrb1, 0, mrb1.length));
        System.out.println(comparator.compare(mrb3, 0, mrb3.length, mrb2, 0, mrb2.length));
        System.out.println(comparator.compare(mrb3, 0, mrb3.length, mrb3, 0, mrb3.length));
        System.out.println(comparator.compare(mrb3, 0, mrb3.length, mrb4, 0, mrb4.length));
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println(comparator.compare(mrb4, 0, mrb4.length, mrb1, 0, mrb1.length));
        System.out.println(comparator.compare(mrb4, 0, mrb4.length, mrb2, 0, mrb2.length));
        System.out.println(comparator.compare(mrb4, 0, mrb4.length, mrb3, 0, mrb3.length));
        System.out.println(comparator.compare(mrb4, 0, mrb4.length, mrb4, 0, mrb4.length));
        System.out.println("------------------------------------------------------------------------------------");
    }

    public static void testComparator1(){
        System.out.println(comparator.compare(mr1, mr1));
        System.out.println(comparator.compare(mr1, mr2));
        System.out.println(comparator.compare(mr1, mr3));
        System.out.println(comparator.compare(mr1, mr4));
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println(comparator.compare(mr2, mr1));
        System.out.println(comparator.compare(mr2, mr2));
        System.out.println(comparator.compare(mr2, mr3));
        System.out.println(comparator.compare(mr2, mr4));
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println(comparator.compare(mr3, mr1));
        System.out.println(comparator.compare(mr3, mr2));
        System.out.println(comparator.compare(mr3, mr3));
        System.out.println(comparator.compare(mr3, mr4));
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println(comparator.compare(mr4, mr1));
        System.out.println(comparator.compare(mr4, mr2));
        System.out.println(comparator.compare(mr4, mr3));
        System.out.println(comparator.compare(mr4, mr4));
        System.out.println("------------------------------------------------------------------------------------");
    }

    public static void testEqualAndHashCode(){
        System.out.println(mr1.equals(mr1));
        System.out.println(mr1.hashCode() == mr1.hashCode());
        System.out.println();
        System.out.println(mr1.equals(mr2));
        System.out.println(mr1.hashCode() == mr2.hashCode());
        System.out.println();
        System.out.println(mr1.equals(mr3));
        System.out.println(mr1.hashCode() == mr3.hashCode());
        System.out.println();
        System.out.println(mr1.equals(mr4));
        System.out.println(mr1.hashCode() == mr4.hashCode());
        System.out.println("---------------------------------------------------------------------------------------------");
        System.out.println(mr2.equals(mr1));
        System.out.println(mr2.hashCode() == mr1.hashCode());
        System.out.println();
        System.out.println(mr2.equals(mr2));
        System.out.println(mr2.hashCode() == mr2.hashCode());
        System.out.println();
        System.out.println(mr2.equals(mr3));
        System.out.println(mr2.hashCode() == mr3.hashCode());
        System.out.println();
        System.out.println(mr2.equals(mr4));
        System.out.println(mr2.hashCode() == mr4.hashCode());
        System.out.println("---------------------------------------------------------------------------------------------");
        System.out.println(mr3.equals(mr1));
        System.out.println(mr3.hashCode() == mr1.hashCode());
        System.out.println();
        System.out.println(mr3.equals(mr2));
        System.out.println(mr3.hashCode() == mr2.hashCode());
        System.out.println();
        System.out.println(mr3.equals(mr3));
        System.out.println(mr3.hashCode() == mr3.hashCode());
        System.out.println();
        System.out.println(mr3.equals(mr4));
        System.out.println(mr3.hashCode() == mr4.hashCode());
        System.out.println("---------------------------------------------------------------------------------------------");
        System.out.println(mr4.equals(mr1));
        System.out.println(mr4.hashCode() == mr1.hashCode());
        System.out.println();
        System.out.println(mr4.equals(mr2));
        System.out.println(mr4.hashCode() == mr2.hashCode());
        System.out.println();
        System.out.println(mr4.equals(mr3));
        System.out.println(mr4.hashCode() == mr3.hashCode());
        System.out.println();
        System.out.println(mr4.equals(mr4));
        System.out.println(mr4.hashCode() == mr4.hashCode());
        System.out.println("---------------------------------------------------------------------------------------------");
    }
}
