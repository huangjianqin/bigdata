package org.kin.bigdata.hadoop.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by 健勤 on 2017/4/22.
 */
public class TwoWordCountJob extends Configured implements Tool{
    public int run(String[] strings) throws Exception {
        //统计一个文件的wordcount
//        getConf().set("mapreduce.output.textoutputformat.separator", ",");
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("TwoWordCountJob1");
        job1.setJarByClass(TwoWordCountJob.class);
        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(WordReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path("data/words.txt"));
        FileOutputFormat.setOutputPath(job1, new Path("result1"));

        //统计一个文件和job1输出的wordcount
        Job job2 = Job.getInstance(getConf());
        job2.setJobName("TwoWordCountJob2");
        job2.setJarByClass(TwoWordCountJob.class);
        job2.setMapperClass(WordMapper.class);
        job2.setReducerClass(WordReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path("data/words.txt"));
        FileInputFormat.addInputPath(job2, new Path("result1"));
        FileOutputFormat.setOutputPath(job2, new Path("result2"));

        //封装为ControlledJob
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());

        //job2依赖于job1的完成
        controlledJob2.addDependingJob(controlledJob1);

        //封装为JobControl
        JobControl jobControl = new JobControl("TwoWordCount");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Thread jobst = new Thread(jobControl);
        jobst.start();

        while(true){
            if(jobControl.allFinished()){
                System.out.println("全部完成,嘻嘻嘻");
                for(ControlledJob job: jobControl.getSuccessfulJobList()){
                    System.out.println(job.toString());
                    System.out.println("-------------------------------------------------------------");
                }
                jobControl.stop();
                return 0;
            }
            else if(jobControl.getFailedJobList().size() > 0){
                System.out.println("有任务失败,嘻嘻嘻");
                System.out.println(jobControl.getFailedJobList().toString());
                jobControl.stop();
                return -1;
            }
            Thread.sleep(1000);
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println(ToolRunner.run(new TwoWordCountJob(), new String[]{""}));
    }
}
