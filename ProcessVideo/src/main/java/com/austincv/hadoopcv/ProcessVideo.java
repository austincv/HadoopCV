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

			//we create a file system object for interacting with the HDFS
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			/*
			 * TODO: enclose this in try catch block
			 * We need to catch errors that might happen
			 * if the local file system is full or in other such cases 
			 */
			//copy the video file to the working directory of the map task
			fs.copyToLocalFile(new Path(filePath.toString()),new Path(fileName.toString()));
			
			/*
			 * TODO: get the path of the code from context property - make it a command line arg
			 * An even better method would be to use the distributed cache for loading the 
			 * python code in which case we don't need to load the code to HDFS first
			 */
			//copy code from the predefined location to the local dir for execution
			fs.copyToLocalFile(new Path("/user/austin/code/code.py"),new Path("code.py"));
			
			//build a process using the python code and run it
			//the python code takes in one parameter - the fileName
			ProcessBuilder pb = new ProcessBuilder("python","code.py",fileName.toString());
			Process p = pb.start();//start the process
			
			/*
			 * TODO: figure out how to process the output of python code
			 * The current implementation just reads the first line from the output
			 * of the python code - need to write a simple loop to read the output
			 */
			//read output from the process
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			Text opencvResult = new Text(in.readLine().toString());
			
			context.write(fileName,opencvResult);
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
		/*
		 * TODO: use the newer API to avoid deprecated warnings
		 * Refer hints given at run time for fixing this
		 */
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
