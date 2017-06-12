package org.bigdata.hadoop.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 健勤 on 2017/4/22.
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String[] strs = value.toString().split(",");
        String[] strs = value.toString().split("\\t");
        if(strs.length > 1){
            context.write(new Text(strs[0]), new LongWritable(Long.valueOf(strs[1])));
        }else{
            context.write(value, new LongWritable(1));
        }
    }

}
