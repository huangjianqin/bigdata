package org.kin.bigdata.hadoop.common.csv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.List;

/**
 * Created by huangjianqin on 2017/9/8.
 */
public class CSVInputFormatTest extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("CSVInputFormatTest");
        job.setJarByClass(CSVInputFormatTest.class);

        job.setInputFormatClass(CSVInputFormat.class);
        job.setOutputFormatClass(LazyOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, NullOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("data/lawsample.csv"));
        //1
        job.setMapperClass(MultithreadedMapper.class);
        MultithreadedMapper.setMapperClass(job, TestMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, 1);
        //2
//        job.setMapperClass(MultithreadedMapper.class);
//        MultithreadedMapper.setMapperClass(job, TestMapper.class);
//        MultithreadedMapper.setNumberOfThreads(job, 4);
        //3
//        job.setMapperClass(TestMapper.class);

        job.setNumReduceTasks(0);

        return job.waitForCompletion(true)? 1 : 0;
    }

    public static void main(String[] args) {

    }

    public static void baseTest() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(FileInputFormat.INPUT_DIR, "data/lawsample.csv");
        Job job = Job.getInstance(conf);
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        CSVInputFormat csvInputFormat = new CSVInputFormat();
        List<InputSplit> inputSplits = csvInputFormat.getSplits(job);
        RecordReader<LongWritable, List<Text>> recordReader = csvInputFormat.createRecordReader(inputSplits.get(0), taskAttemptContext);

        //begin
        while(recordReader.nextKeyValue()){
            System.out.println(recordReader.getProgress());
            System.out.println(recordReader.getCurrentKey().get());
            for(Text text: recordReader.getCurrentValue()){
                System.out.print(text.toString() + ",");
            }
            System.out.println();
        }

        recordReader.close();
    }
}

class TestMapper extends Mapper<LongWritable, List<Text>, LongWritable, List<Text>>{
    @Override
    protected void map(LongWritable key, List<Text> value, Context context) throws IOException, InterruptedException {
        System.out.println("Mapper...");
        for(Text text: value){
            System.out.print(text.toString() + ",");
        }
        System.out.println();
        System.out.println("Mapper out");
    }
}
