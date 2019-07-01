package org.kin.hadoop.common;

import org.apache.hadoop.io.Text;
import org.kin.hadoop.common.writable.CollectionWritable;
import org.kin.hadoop.common.writable.TextCollectionWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangjianqin on 2017/9/7.
 */
public class TextCollectionWritableTest extends WritableTestBase{
    static List<Text> l1 = new ArrayList<>();
    static List<Text> l2 = new ArrayList<>();
    static List<Text> l3 = new ArrayList<>();
    static List<Text> l4 = new ArrayList<>();

    static CollectionWritable cw1;
    static CollectionWritable cw2;
    static CollectionWritable cw3;
    static CollectionWritable cw4;

    static TextCollectionWritable.TextCollectionComparator comparator = new TextCollectionWritable.TextCollectionComparator();

    static byte[] cwb1;
    static byte[] cwb2;
    static byte[] cwb3;
    static byte[] cwb4;

    static {
        l1.add(new Text("aaa"));
        l1.add(new Text("bbb"));
        l1.add(new Text("ccc"));
        l1.add(new Text("ddd"));

        l2.add(new Text("aaa"));
        l2.add(new Text("bbb"));
        l2.add(new Text("ccc"));
        l2.add(new Text("ddd"));

        l3.add(new Text("aaa"));
        l3.add(new Text("bbb"));
        l3.add(new Text("ccc"));

        l4.add(new Text("aaa"));
        l4.add(new Text("bbb"));
        l4.add(new Text("ccc"));
        l4.add(new Text("ddd"));
        l4.add(new Text("eee"));

        cw1 = new TextCollectionWritable(l1);
        cw2 = new TextCollectionWritable(l2);
        cw3 = new TextCollectionWritable(l3);
        cw4 = new TextCollectionWritable(l4);

        try {
            cwb1 = serialize(cw1);
            cwb2 = serialize(cw2);
            cwb3 = serialize(cw3);
            cwb4 = serialize(cw4);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        testEqualAndHashCode();
//        testComparator1();
//        testComparator2();
//        testCommonOperation();
    }

    public static void testCommonOperation(){
        cw1.addAll(cw2);
        System.out.println(cw1.toString());
        cw1.removeAll(cw2);
        System.out.println(cw1.toString());
        System.out.println(cw1.containsAll(cw2));
        System.out.println(cw1.contains(new Text("aaa")));
        cw1.clear();
        System.out.println(cw1.toString());
    }

    public static void testComparator2(){
        System.out.println("---------------------------bytes.length---------------------------------------");
        System.out.println(cwb1.length);
        System.out.println(cwb2.length);
        System.out.println(cwb3.length);
        System.out.println(cwb4.length);
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cw1, cw1));
        System.out.println(comparator.compare(cw1, cw2));
        System.out.println(comparator.compare(cw1, cw3));
        System.out.println(comparator.compare(cw1, cw4));
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cw2, cw1));
        System.out.println(comparator.compare(cw2, cw2));
        System.out.println(comparator.compare(cw2, cw3));
        System.out.println(comparator.compare(cw2, cw4));
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cw3, cw1));
        System.out.println(comparator.compare(cw3, cw2));
        System.out.println(comparator.compare(cw3, cw3));
        System.out.println(comparator.compare(cw3, cw4));
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cw4, cw1));
        System.out.println(comparator.compare(cw4, cw2));
        System.out.println(comparator.compare(cw4, cw3));
        System.out.println(comparator.compare(cw4, cw4));
        System.out.println("-----------------------------------------------------------------------------");
    }

    public static void testComparator1(){
        System.out.println("---------------------------bytes.length---------------------------------------");
        System.out.println(cwb1.length);
        System.out.println(cwb2.length);
        System.out.println(cwb3.length);
        System.out.println(cwb4.length);
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cwb1, 0, cwb1.length, cwb1, 0, cwb1.length));
        System.out.println(comparator.compare(cwb1, 0, cwb1.length, cwb2, 0, cwb2.length));
        System.out.println(comparator.compare(cwb1, 0, cwb1.length, cwb3, 0, cwb3.length));
        System.out.println(comparator.compare(cwb1, 0, cwb1.length, cwb4, 0, cwb4.length));
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cwb2, 0, cwb2.length, cwb1, 0, cwb1.length));
        System.out.println(comparator.compare(cwb2, 0, cwb2.length, cwb2, 0, cwb2.length));
        System.out.println(comparator.compare(cwb2, 0, cwb2.length, cwb3, 0, cwb3.length));
        System.out.println(comparator.compare(cwb2, 0, cwb2.length, cwb4, 0, cwb4.length));
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cwb3, 0, cwb3.length, cwb1, 0, cwb1.length));
        System.out.println(comparator.compare(cwb3, 0, cwb3.length, cwb2, 0, cwb2.length));
        System.out.println(comparator.compare(cwb3, 0, cwb3.length, cwb3, 0, cwb3.length));
        System.out.println(comparator.compare(cwb3, 0, cwb3.length, cwb4, 0, cwb4.length));
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(comparator.compare(cwb4, 0, cwb4.length, cwb1, 0, cwb1.length));
        System.out.println(comparator.compare(cwb4, 0, cwb4.length, cwb2, 0, cwb2.length));
        System.out.println(comparator.compare(cwb4, 0, cwb4.length, cwb3, 0, cwb3.length));
        System.out.println(comparator.compare(cwb4, 0, cwb4.length, cwb4, 0, cwb4.length));
        System.out.println("-----------------------------------------------------------------------------");
    }

    public static void testEqualAndHashCode(){
        System.out.println(cw1.equals(cw1));
        System.out.println(cw1.hashCode() == cw1.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw1.equals(cw2));
        System.out.println(cw1.hashCode() == cw2.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw1.equals(cw3));
        System.out.println(cw1.hashCode() == cw3.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw1.equals(cw4));
        System.out.println(cw1.hashCode() == cw4.hashCode());
        System.out.println("-----------------------------------------------------------------------------");

        System.out.println();

        System.out.println(cw2.equals(cw1));
        System.out.println(cw2.hashCode() == cw1.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw2.equals(cw2));
        System.out.println(cw2.hashCode() == cw2.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw2.equals(cw3));
        System.out.println(cw2.hashCode() == cw3.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw2.equals(cw4));
        System.out.println(cw2.hashCode() == cw4.hashCode());
        System.out.println("-----------------------------------------------------------------------------");

        System.out.println();

        System.out.println(cw3.equals(cw1));
        System.out.println(cw3.hashCode() == cw1.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw3.equals(cw2));
        System.out.println(cw3.hashCode() == cw2.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw3.equals(cw3));
        System.out.println(cw3.hashCode() == cw3.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw3.equals(cw4));
        System.out.println(cw3.hashCode() == cw4.hashCode());
        System.out.println("-----------------------------------------------------------------------------");

        System.out.println();

        System.out.println(cw4.equals(cw1));
        System.out.println(cw4.hashCode() == cw1.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw4.equals(cw2));
        System.out.println(cw4.hashCode() == cw2.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw4.equals(cw3));
        System.out.println(cw4.hashCode() == cw3.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println(cw4.equals(cw4));
        System.out.println(cw4.hashCode() == cw4.hashCode());
        System.out.println("-----------------------------------------------------------------------------");
    }
}
