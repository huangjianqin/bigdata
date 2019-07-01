package org.kin.hadoop.common.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

/**
 * Created by 健勤 on 2017/5/22.
 */
public class TextPairWritable extends PairWritable<Text, Text> {
    public TextPairWritable() {
        super(new Text(), new Text());
    }

    public TextPairWritable(Text text, Text text2) {
        super(text, text2);
    }

    static {
        //为该Writable注册Comparator
        WritableComparator.define(TextPairWritable.class, new TextPairComparator());
    }

    public static class TextPairComparator extends WritableComparator {
        public TextPairComparator() {
            super(TextPairWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1Size = WritableUtils.decodeVIntSize(b1[s1]);
                int firstL2Size = WritableUtils.decodeVIntSize(b2[s2]);
                int firstL1 = firstL1Size + readVInt(b1, s1);
                int firstL2 = firstL2Size + readVInt(b2, s2);
                int cmd1 = compareBytes(b1, s1 + firstL1Size, firstL1, b2, s2 + firstL2Size, firstL2);
                if (cmd1 != 0) {
                    return cmd1;
                }

                int secondL1Size = WritableUtils.decodeVIntSize(b1[s1 + firstL1]);
                int secondL2Size = WritableUtils.decodeVIntSize(b2[s2 + firstL2]);

                return compareBytes(b1, s1 + firstL1 + secondL1Size, l1 - firstL1 - secondL1Size,
                        b2, s2 + firstL2 + secondL2Size, l2 - firstL2 - secondL2Size);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return -1;
        }
    }
}
