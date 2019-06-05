package org.kin.bigdata.hadoop.airlineclientdiscovery;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by huangjianqin on 2017/8/26.
 */
public class PreparationJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
//        getConf().set(LazyOutputFormat.OUTPUT_FORMAT, "org.apache.hadoop.mapreduce.lib.output.FileOutputFormat");
        Job job = Job.getInstance(getConf());
        job.setJarByClass(PreparationJob.class);
        job.setJobName("PreparationJob");
        job.setMapperClass(PreparationMapper.class);
        job.setOutputFormatClass(LazyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("data/airlineData.csv"));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("result"));
        job.setOutputKeyClass(NullWritable.class);
        job.waitForCompletion(false);
        return job.isSuccessful() ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(ToolRunner.run(new PreparationJob(), args));
    }
}
