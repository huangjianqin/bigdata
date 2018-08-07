package org.kin.bigdata.hadoop.common.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by huangjianqin on 2017/9/5.
 */
public class TextCollectionWritable extends CollectionWritable<Text> {
    public TextCollectionWritable() {
        super(Text.class);
    }

    public TextCollectionWritable(Collection<? extends Text> collection) {
        super(Text.class, (Collection<Text>) collection, false);
    }

    protected TextCollectionWritable(Collection<? extends Text> collection, boolean isOverwrite){
        super(Text.class, (Collection<Text>) collection, isOverwrite);
    }

    public TextCollectionWritable(Text... texts) {
        this(Arrays.asList(texts));
    }

    public TextCollectionWritable(String... strs) {
        super(Text.class, Collections.EMPTY_LIST, false);
        super.addAll(Arrays.asList(str2Text(strs)));
    }

    private Text[] str2Text(String... strs){
        Text[] result = new Text[strs.length];
        for(int i = 0; i < strs.length; i++){
            result[i] = new Text(strs[i]);
        }
        return result;
    }

    static {
        WritableComparator.define(TextCollectionWritable.class, new TextCollectionComparator());
    }

    public static class TextCollectionComparator extends WritableComparator{
        public TextCollectionComparator() {
            super(TextCollectionWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            Integer b1L = readInt(b1, s1);
            Integer b2L = readInt(b2, s2);
            Integer lCMD = b1L.compareTo(b2L);
            if(lCMD != 0){
                return lCMD;
            }
            // same size
            // size class is int, so 4 bytes
            Integer b1RealS = s1 + 4;
            Integer b2RealS = s2 + 4;

            //Text底层bytes组成是vint+valueBytes
            for(int i = 0; i < b1L; i++){
                try {
                    Integer VIntSize1 = WritableUtils.decodeVIntSize(b1[b1RealS]);
                    Integer VIntSize2 = WritableUtils.decodeVIntSize(b2[b2RealS]);
                    Integer valueL1 = VIntSize1 + readVInt(b1, b1RealS);
                    Integer valueL2 = VIntSize2 + readVInt(b2, b2RealS);

                    Integer cmd = Integer.MAX_VALUE;
                    if(i < b1L - 1){
                        cmd = compareBytes(b1, b1RealS + VIntSize1, valueL1, b2, b2RealS + VIntSize2, valueL2);
                    }
                    else if(i == b1L - 1){
                        //the last
                        //此处不能=length,要length-1.非末尾情况,读取s1<=pos<l1
                        cmd = compareBytes(b1, b1RealS + VIntSize1, valueL1 - 1, b2, b2RealS + VIntSize2, valueL2 - 1);
                    }

                    if(cmd == Integer.MAX_VALUE){
                        throw new IllegalStateException("something wrong");
                    }
                    if(cmd != 0){
                        return cmd;
                    }
                    //更新下一个value item的起点
                    b1RealS += valueL1;
                    b2RealS += valueL2;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return 0;
        }
    }

}
