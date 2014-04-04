package com.austincv.hadoopcv;

/*
 * This is similar to WholeFileInputFormat
 * Expect each image to go to one Mapper
 * You get key as NullWritable and the value as BytesWritable
 */

import com.austincv.hadoopcv.PathFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
 
public class PathFileInputFormat extends FileInputFormat<Text, Text> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
 
    @Override
    public RecordReader<Text, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
        return new PathFileRecordReader();
    }
}