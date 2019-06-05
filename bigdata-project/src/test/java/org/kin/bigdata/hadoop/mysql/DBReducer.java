package org.kin.bigdata.hadoop.mysql;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 健勤 on 2017/7/19.
 */
public class DBReducer extends Reducer<LongWritable, UserWritable, UserWritable, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<UserWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<UserWritable> iterator = values.iterator();
        context.write(iterator.next(), new Text("1111"));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
