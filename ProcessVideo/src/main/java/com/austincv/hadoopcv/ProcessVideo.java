package com.austincv.hadoopcv;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.austincv.hadoopcv.PathFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProcessVideo extends Configuration implements Tool {

	public static class ProcessVideoMapper extends Mapper<Text, Text, Text, Text> {

	
		public void map(Text fileName, Text filePath, Context context) throws IOException, InterruptedException{

			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			//TODO: enclose this in try catch block
			fs.copyToLocalFile(new Path(filePath.toString()),new Path(fileName.toString()));
			//TODO: get the path of the code from context property - make it a command line arg
			fs.copyToLocalFile(new Path("/user/austin/code/code.py"),new Path("code.py"));
			
			ProcessBuilder pb = new ProcessBuilder("python","code.py",fileName.toString());
			Process p = pb.start();
			
			//TODO: figure out how to process the output of python code
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			Text fps = new Text(in.readLine().toString());
			
			context.write(fileName,fps);
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
		//TODO: use the newer api to avoid depricated warnings
		Job readImageJob = new Job(getConf(),"Video analysis job");
		readImageJob.setJarByClass(ProcessVideo.class);
		readImageJob.setMapperClass(ProcessVideoMapper.class);

		readImageJob.setInputFormatClass(PathFileInputFormat.class);
		readImageJob.setMapOutputKeyClass(Text.class);
		readImageJob.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(readImageJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(readImageJob, new Path(args[1]));
		
		if(readImageJob.waitForCompletion(true))
			return 1;
		else 
			return 0;
	}
	
	public static void main (String[] args) throws Exception{
		
		ToolRunner.run(new Configuration(), new ProcessVideo(), args);
	}

}
