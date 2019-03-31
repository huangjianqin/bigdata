package org.kin.hadoop.common.csv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.kin.hadoop.common.writable.TextCollectionWritable;

import java.io.IOException;

/**
 * Created by huangjianqin on 2017/9/4.
 * base on opencsv
 */
public class CSVInputFormat extends FileInputFormat<LongWritable, TextCollectionWritable> {
    public static final String IS_ZIPFILE = "mapreduce.csvinput.zipfile";
    public static final boolean DEFAULT_ZIP = false;

    @Override
    public RecordReader<LongWritable, TextCollectionWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        RecordReader<LongWritable, TextCollectionWritable> recordReader = new CSVRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}
