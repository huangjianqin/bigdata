package org.kin.bigdata.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 健勤 on 2017/5/20.
 */
public class Split extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        getConf().set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(getConf());
        job.setJarByClass(Split.class);
        job.setJobName("Split");
        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(SplitReducer.class);
        FileInputFormat.addInputPath(job, new Path("data/law.csv"));
        FileOutputFormat.setOutputPath(job, new Path("law"));

        job.waitForCompletion(true);
        System.out.println(job.isSuccessful());
        return job.getJobState().getValue();
    }

    public static void main(String[] args) throws Exception {
        System.out.println(ToolRunner.run(new Split(), args));
    }
}

class SplitMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(1), value);
    }
}

class SplitReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
    private static final int num = 100;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        for(int i = 0; i < num && iterator.hasNext(); i++){
            context.write(iterator.next(), new LongWritable(i));
        }
    }
}
