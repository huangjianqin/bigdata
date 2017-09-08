package org.kin.bigdata.hadoop.common.writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

/**
 * Created by 健勤 on 2017/5/21.
 * 比较item<? extends WritableComparable>
 */
public class PairItemComparator extends WritableComparator {
    public PairItemComparator(Class<? extends WritableComparable> claxx) {
        super(claxx);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            int length1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
            int length2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
            return compareBytes(b1, s1 + length1, l1 - length1, b2, s2 + length2, l2 - length2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }
}