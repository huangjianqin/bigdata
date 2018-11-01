package org.kin.bigdata.hadoop.common.writable;

import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.TreeSet;

/**
 * Created by huangjianqin on 2017/9/7.
 */
public class TextSortedCollectionWritable extends TextCollectionWritable {
    public TextSortedCollectionWritable() {
    }

    public TextSortedCollectionWritable(Collection<? extends Text> collection) {
        super(new TreeSet<Text>(collection), true);
    }
}
