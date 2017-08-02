package org.kin.bigdata.hadoop.LawAnalysis;

import org.apache.hadoop.io.Text;

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
}
