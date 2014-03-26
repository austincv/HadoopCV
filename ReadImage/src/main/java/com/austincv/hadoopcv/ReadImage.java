package com.austincv.hadoopcv;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ReadImage extends Configuration implements Tool {

	public static class ReadImageMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		}
	
	}
	
	@Override
	public void setConf(Configuration conf) {
		// Auto-generated method stub
		
	}

	@Override
	public Configuration getConf() {
		return new Configuration();
	}

	@Override
	public int run(String[] args) throws Exception {
		// Auto-generated method stub
		Job readImageJob = new Job(getConf(),"Read Images");
		readImageJob.setJarByClass(ReadImage.class);
		readImageJob.setMapperClass(ReadImageMapper.class);

		readImageJob.setInputFormatClass(TextInputFormat.class);
		readImageJob.setMapOutputKeyClass(IntWritable.class);
		readImageJob.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(readImageJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(readImageJob, new Path(args[1]));
		
		if(readImageJob.waitForCompletion(true))
			return 1;
		else 
			return 0;
	}
	
	public static void main (String[] args) throws Exception{
		
		ToolRunner.run(new Configuration(), new ReadImage(), args);
	}

}