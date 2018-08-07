package org.kin.bigdata.hadoop.common.csv;

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.kin.bigdata.hadoop.common.writable.TextCollectionWritable;

import java.io.*;
import java.util.zip.ZipInputStream;

import static org.kin.bigdata.hadoop.common.csv.CSVInputFormat.DEFAULT_ZIP;
import static org.kin.bigdata.hadoop.common.csv.CSVInputFormat.IS_ZIPFILE;

/**
 * Created by huangjianqin on 2017/9/4.
 */
public class CSVRecordReader extends RecordReader<LongWritable, TextCollectionWritable> {
    private final CSVParser parser = new CSVParser();
    private LongWritable cKey;
    private TextCollectionWritable cValue;

    private boolean isZipFile;
    private long start;
    private long pos;
    private long end;
    protected Reader reader;
    private InputStream is;
    private CompressionCodecFactory compressionCodeces = null;

    public CSVRecordReader() {
    }

    public CSVRecordReader(InputStream is, Configuration conf) {
        try {
            init(is, conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        Configuration job = taskAttemptContext.getConfiguration();

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();

        Path file = fileSplit.getPath();
        compressionCodeces = new CompressionCodecFactory(job);
        CompressionCodec compressionCodec = compressionCodeces.getCodec(file);

        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIs = fs.open(fileSplit.getPath());

        if(compressionCodec != null){
            InputStream is = compressionCodec.createInputStream(fileIs);
            end = Long.MAX_VALUE;
        }
        else{
            if(start != 0){
                fileIs.seek(start);
            }
            is = fileIs;
        }

        pos = start;
        init(is, job);
    }

    private void init(InputStream is, Configuration conf) throws IOException {
        isZipFile = conf.getBoolean(IS_ZIPFILE, DEFAULT_ZIP);
        if(isZipFile){
            ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is));
            zis.getNextEntry();
            is = zis;
        }
        this.is = is;
        reader = new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(cKey == null){
            cKey = new LongWritable();
        }
        cKey.set(pos);

        if(cValue == null){
            cValue = new TextCollectionWritable();
        }

        while (true) {
            if (pos >= end)
                return false;
            long newSize = 0;
            newSize = readLine(cValue);
            pos += newSize;
            if (newSize == 0) {
                if (isZipFile) {
                    ZipInputStream zis = (ZipInputStream) is;
                    if (zis.getNextEntry() != null) {
                        is = zis;
                        reader = new BufferedReader(new InputStreamReader(is));
                        continue;
                    }
                }
                cKey = null;
                cValue = null;
                return false;
            } else {
                return true;
            }
        }
    }

    private long readLine(TextCollectionWritable cValue) throws IOException {
        //clean tha last csv values
        cValue.clear();

        //read a line
        long readed = 0L;
        int i;
        StringBuilder sb = new StringBuilder();
        while((i = reader.read()) != -1){
            char c = (char) i;
            readed ++;
            sb.append(c);
            if(c == '\n'){
                break;
            }
        }
        //have content % \n
        if(sb.length() > 0 && sb.indexOf("\n") == sb.length() - 1){
            //remove \n
            sb.replace(sb.length() - 1, sb.length(), "");
            if(sb.indexOf("\r") == sb.length() - 1){
                //windows下编辑的文件
                //remove \r
                sb.replace(sb.length() - 1, sb.length(), "");
            }
        }

        String csvLine = sb.toString();

        //parser a csvLine and fill the container
        for(String csvItem: parser.parseLine(csvLine)){
            cValue.add(new Text(csvItem));
        }

        return readed;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return cKey;
    }

    @Override
    public TextCollectionWritable getCurrentValue() throws IOException, InterruptedException {
        return cValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start == end){
            return 0.0f;
        }
        else{
            return Math.min(1.0f, (pos - start)/(float)(end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if(reader != null){
            reader.close();
            reader = null;
        }

        if(is != null){
            is.close();
            is = null;
        }

    }
}
