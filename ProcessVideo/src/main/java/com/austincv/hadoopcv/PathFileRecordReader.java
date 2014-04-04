package com.austincv.hadoopcv;

import java.io.IOException;

import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
 
import org.apache.hadoop.fs.Path;

 
class PathFileRecordReader extends RecordReader<Text, Text> {
  
    private FileSplit fileSplit;
    private boolean processed = false;
  
    private Text key = new Text();
    private Text value = new Text();
 
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
    }
 
    public boolean nextKeyValue() throws IOException {
        if (!processed) {
            
        	Path filePath = fileSplit.getPath();
            value.set(filePath.toString());
            key.set(filePath.getName());

            processed = true;
            return true;
        }
        return false;
    }
 
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }
 
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException  {
        return processed ? 1.0f : 0.0f;
    }
 
    @Override
    public void close() throws IOException {
        // do nothing
    }
}