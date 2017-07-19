package org.bigdata.hadoop.db;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;

/**
 * Created by 健勤 on 2017/7/19.
 */
public class DBMppaer extends Mapper<LongWritable, UserWritable, LongWritable, UserWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, UserWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, new UserWritable(value.getId() + 10, value.getName() + "1", value.getDescription()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}

