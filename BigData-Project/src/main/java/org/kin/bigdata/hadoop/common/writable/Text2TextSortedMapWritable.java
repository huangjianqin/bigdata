package org.kin.bigdata.hadoop.common.writable;

import org.apache.hadoop.io.Text;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by huangjianqin on 2017/9/7.
 */
public class Text2TextSortedMapWritable extends Text2TextMapWritable{
    public Text2TextSortedMapWritable() {
    }

    public Text2TextSortedMapWritable(Map<Text, Text> map) {
        super(new TreeMap<>(map), true);
    }
}
