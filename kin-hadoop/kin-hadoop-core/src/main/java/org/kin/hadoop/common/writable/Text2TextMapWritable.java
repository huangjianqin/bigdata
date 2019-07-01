package org.kin.hadoop.common.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/5.
 */
public class Text2TextMapWritable extends MapWritable<Text, Text> {
    public Text2TextMapWritable() {
        super(Text.class, Text.class);
    }

    public Text2TextMapWritable(Map<Text, Text> map) {
        super(Text.class, Text.class, map, false);
    }

    protected Text2TextMapWritable(Map<Text, Text> map, boolean isOverwrite) {
        super(Text.class, Text.class, map, isOverwrite);
    }

    static {
        WritableComparator.define(Text2TextMapWritable.class, new Text2TextMapComparator());
    }

    public static class Text2TextMapComparator extends WritableComparator {
        public Text2TextMapComparator() {
            super(Text2TextMapWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            Integer b1Length = readInt(b1, s1);
            Integer b2Length = readInt(b2, s2);
            Integer lCMD = b1Length.compareTo(b2Length);
            if (lCMD != 0) {
                return lCMD;
            }
            // same size
            // size class is int, so 4 bytes
            Integer b1RealSize = s1 + 4;
            Integer b2RealSize = s2 + 4;

            //Text底层bytes组成是vint+valueBytes
            //按key排序
            for (int i = 0; i < b1Length; i++) {
                try {
                    Integer VIntSize1 = WritableUtils.decodeVIntSize(b1[b1RealSize]);
                    Integer VIntSize2 = WritableUtils.decodeVIntSize(b2[b2RealSize]);
                    Integer valueL1 = VIntSize1 + readVInt(b1, b1RealSize);
                    Integer valueL2 = VIntSize2 + readVInt(b2, b2RealSize);


                    Integer cmd = Integer.MAX_VALUE;
                    if (i < b1Length - 1) {
                        cmd = compareBytes(b1, b1RealSize + VIntSize1, valueL1, b2, b2RealSize + VIntSize2, valueL2);
                    } else if (i == b1Length - 1) {
                        //the last
                        cmd = compareBytes(b1, b1RealSize + VIntSize1, valueL1 - 1, b2, b2RealSize + VIntSize2, valueL2 - 1);
                    }

                    if (cmd == Integer.MAX_VALUE) {
                        throw new IllegalStateException("something wrong");
                    }

                    if (cmd != 0) {
                        return cmd;
                    }
                    //更新下一个value item的起点
                    //跳过value
                    b1RealSize += valueL1;
                    b2RealSize += valueL2;
                    b1RealSize += WritableUtils.decodeVIntSize(b1[b1RealSize]) + readVInt(b1, b1RealSize);
                    b2RealSize += WritableUtils.decodeVIntSize(b2[b2RealSize]) + readVInt(b2, b2RealSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return 0;
        }
    }
}
