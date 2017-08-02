package org.kin.bigdata.hadoop.LawAnalysis;

import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by 健勤 on 2017/5/21.
 */
public class PairWritableTest extends WritableTestBase {
    public static void main(String[] args) throws IOException {
        PairWritable<Text, Text> textPair1 = new PairWritable<>(new Text("aaa"), new Text("bbb"));
        PairWritable<Text, Text> textPair2 = new PairWritable<>(new Text("aaa"), new Text("bbb"));
        PairWritable<Text, Text> textPair3 = new PairWritable<>(new Text("bbb"), new Text("aaa"));

//        printlnInfo(textPair1, textPair2);
//        textPair1.setFirst(new Text("bbb"));
//        printlnInfo(textPair1, textPair2);
//        textPair1.setSecond(new Text("aaa"));
//        printlnInfo(textPair1, textPair2);

        byte[] b1 = serialize(textPair1);
        byte[] b2 = serialize(textPair2);
        byte[] b3 = serialize(textPair3);

        PairWritable.PairComparator comparator = new PairWritable.PairComparator();
        System.out.println(comparator.compare(textPair1, textPair2));
        System.out.println(comparator.compare(textPair1, textPair3));
        System.out.println(comparator.compare(textPair2, textPair3));
        System.out.println("---------------------------------------------------------------------");

        System.out.println(comparator.compare(textPair2, textPair1));
        System.out.println(comparator.compare(textPair3, textPair1));
        System.out.println(comparator.compare(textPair3, textPair2));
        System.out.println("---------------------------------------------------------------------");

        System.out.println(comparator.compare(b1, 0, b1.length, b2, 0, b2.length));
        System.out.println(comparator.compare(b1, 0, b1.length, b3, 0, b3.length));
        System.out.println(comparator.compare(b2, 0, b2.length, b3, 0, b3.length));
        System.out.println("---------------------------------------------------------------------");

        System.out.println(comparator.compare(b2, 0, b2.length, b1, 0, b1.length));
        System.out.println(comparator.compare(b3, 0, b3.length, b1, 0, b1.length));
        System.out.println(comparator.compare(b3, 0, b3.length, b2, 0, b2.length));
        System.out.println("---------------------------------------------------------------------");

        byte[] b4 = serialize(new Text("aaa"));
        byte[] b5 = serialize(new Text("bbb"));

        PairItemComparator pairItemComparator = new PairItemComparator(Text.class);
        System.out.println(pairItemComparator.compare(new Text("aaa"), new Text("bbb")));
        System.out.println(pairItemComparator.compare(new Text("bbb"), new Text("aaa")));
        System.out.println("---------------------------------------------------------------------");

        System.out.println(comparator.compare(b4, 0, b4.length, b5, 0, b5.length));
        System.out.println(comparator.compare(b5, 0, b5.length, b4, 0, b4.length));
        System.out.println("---------------------------------------------------------------------");

    }

    public static void printlnInfo(PairWritable<Text, Text> textPair1, PairWritable<Text, Text> textPair2){
        System.out.println(textPair1.getFirst());
        System.out.println(textPair1.getSecond());
        System.out.println(textPair1.getPair()[0]);
        System.out.println(textPair1.getPair()[1]);
        System.out.println(textPair1.hashCode() == textPair2.hashCode());
        System.out.println(textPair1.equals(textPair2));
        System.out.println(textPair1.compareTo(textPair2));
        System.out.println("---------------------------------------------------------------------");
    }

}
