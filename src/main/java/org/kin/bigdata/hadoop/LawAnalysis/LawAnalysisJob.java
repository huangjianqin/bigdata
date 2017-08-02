package org.kin.bigdata.hadoop.LawAnalysis;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 健勤 on 2017/5/21.
 */
public class LawAnalysisJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        getConf().set("mapreduce.output.textoutputformat.separator", ",");
        getConf().set("mapreduce.output.lazyoutputformat.outputformat", "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat");
        Job job = Job.getInstance(getConf());
        job.setJarByClass(LawAnalysisJob.class);
        job.setJobName("LawAnalysis");
        job.setMapperClass(LawAnalysisMapper.class);
        job.setMapOutputKeyClass(TextPairWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(LawAnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(LazyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("data/law.csv"));
        FileOutputFormat.setOutputPath(job, new Path("lawanalysis"));

        job.waitForCompletion(true);
        System.out.println(job.isSuccessful());
        System.out.println("----------------------------------------------------------------------------------------------------------");
        System.out.println("输入记录数: " + job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
        System.out.println("----------------------------------------------------------------------------------------------------------");
        System.out.println(job.getCounters().findCounter(FileInputFormatCounter.BYTES_READ).getValue());
        System.out.println("----------------------------------------------------------------------------------------------------------");
        return job.getJobState().getValue();
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(ToolRunner.run(new LawAnalysisJob(), args));
        System.out.println(ToolRunner.run(new LawStatisticsJob(), args));
//        String utf8Value = "\"9\",776247310,140100,\"Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_1 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/12B435 UCBrowser/10.2.5.551 Mobile\",\"iOS\",\"1577666249.1422457401\",\"1577666249.1422457401\",1422973317133,\"2015-02-03 22:21:57\",\"/ask/question_77070.html\",20150203,\"http://www.lawtime.cn/ask/question_77070.html\",\"101003\",\"www.lawtime.cn\",\"请问事业单位退休死亡人员抚恤金发放按什么规定执行，可按民发【2007】64号文件执行吗？ - 法律快车法律咨询\",62,\"劳动合同纠纷\",\"法律咨询\",\"zhidao.baidu.com/question/184482114.html\",\"http://zhidao.baidu.com/question/184482114.html?mzl=qb_xg_0&mzl_jy=0&word=%E6%B0%91%E5%8F%912007%E5%B9%B464%E5%8F%B7%E6%96%87%E4%BB%B6&hitRelateOptimi=&mybabytest=0&ad_test=&qb_20per=5&from=2001a&ssid=0&uid=0&fr=wenda_ala&tj=wenda_1_0_10_title&step=2\",NA,\"zhidao.baidu.com\"";
//                //"\"1\",2683657840,140100,\"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36 SE 2.X MetaSr 1.0\",\"Windows XP\",\"785022225.1422973265\",\"785022225.1422973265\",1422973268278,\"2015-02-03 22:21:08\",\"/info/hunyin/hunyinfagui/201404102884290_6.html\",20150203,\"http://www.lawtime.cn/info/hunyin/hunyinfagui/201404102884290_6.html\",\"107001\",\"www.lawtime.cn\",\"广东省人口与计划生育条例全文2014 - 法律快车婚姻法\",31,\"故意伤害\",\"计划生育\",NA,NA,NA,NA\n";
//
//        //过滤每行记录第一个的字符
//        utf8Value = utf8Value.split(",", 2)[1];
//
//        int count = 1;
//        String[] splits = null;
//        while(utf8Value.contains(",")){
//            splits = utf8Value.split(",", 2);
//            String full = "";
//            if(splits[0].matches("\".*\"")){
//                //匹配,"XXXX",
//                full = splits[0];
//            }else if(splits[0].matches("\".*")){
//                //匹配,"XXX,XXX,XXX",
//                full = splits[0] + ",";
//                splits = splits[1].split(",", 2);
//
//                while(!splits[0].matches(".*\"")){
//                    full += splits[0] + ",";
//                    splits = splits[1].split(",", 2);
//                }
//                full += splits[0];
//            }
//            else{
//                //匹配,XXXX,
//                full = splits[0];
//            }
//            System.out.println(full.replaceAll("\"", ""));
//            count ++;
//            utf8Value = splits[1];
//        }
//        System.out.println(splits[1].replaceAll("\"", ""));
//        System.out.println(count);
    }
}

class LawAnalysisMapper extends Mapper<LongWritable, Text, TextPairWritable, LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String utf8Value = value.toString();

        //过滤每行记录第一个的表示数字
        if(utf8Value.split(",").length >= 22){
            utf8Value = utf8Value.split(",", 2)[1];
        }

        int count = 1;
        String[] splits = null;
        while(utf8Value.contains(",")){
            splits = utf8Value.split(",", 2);
            String full = "";
            if(splits[0].matches("\".*\"")){
                //匹配,"XXXX",
                full = splits[0];
            }else if(splits[0].matches("\".*")){
                //匹配,"XXX,XXX,XXX",
                full = splits[0] + ",";
                splits = splits[1].split(",", 2);

                while(!splits[0].matches(".*\"")){
                    full += splits[0] + ",";
                    splits = splits[1].split(",", 2);
                }
                full += splits[0];
            }
            else{
                //匹配,XXXX,
                full = splits[0];
            }
            context.write(PairWritableFactory.textPair(count + "", full.replaceAll("\"", "")), new LongWritable(1));
            count ++;
            utf8Value = splits[1];
        }
        if(splits != null){
            context.write(PairWritableFactory.textPair(count + "", splits[1].replaceAll("\"", "")), new LongWritable(1));
        }
    }
}

class LawAnalysisReducer extends Reducer<TextPairWritable, LongWritable, Text, LongWritable>{
    @Override
    protected void reduce(TextPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        Iterator<LongWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }
        context.write(new Text(key.getFirst().toString() + "," + key.getSecond().toString()), new LongWritable(sum));
    }
}

class LawStatisticsJob extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {
        getConf().set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(getConf());
        job.setJarByClass(LawStatisticsJob.class);
        job.setJobName("LawStatistics");
        job.setMapperClass(LawStatisticsMapper.class);
        job.setReducerClass(LawStatisticsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path("lawanalysis"));
        FileOutputFormat.setOutputPath(job, new Path("lawstatistics"));

        job.waitForCompletion(true);
        System.out.println(job.isSuccessful());
        System.out.println("----------------------------------------------------------------------------------------------------------");
        System.out.println("输入记录数: " + job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
        System.out.println("----------------------------------------------------------------------------------------------------------");

        return job.getJobState().getValue();
    }
}

class LawStatisticsMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        context.write(new Text(values[0]), new LongWritable(Long.valueOf(values[values.length - 1])));
    }
}

class LawStatisticsReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        Iterator<LongWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            sum += iterator.next().get();
        }
        context.write(new Text(key), new LongWritable(sum));
    }
}
