package org.gstry.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.gstry.weather.parser.WeatherRecordFilterMapper;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.gstry.weather.writable.WeatherRecordWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherFileRecorder extends Configured implements Tool{

	private static final Logger LOG = LoggerFactory.getLogger(WeatherFileRecorder.class);
	private static final String APP_ID = "WEATHER RECORD PARSER";

	@Override
	public int run(String[] args) throws Exception {		
		if(args.length!=2){
			System.err.printf("Usage :%s [generic options] <directory_name> <opfile_name>",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Path dirPath = new Path(args[0]);		
		Path sequenceFileOutputPath = new Path(args[1]); 
		Job job = createJob(this,getConf(),dirPath,sequenceFileOutputPath);
		if(job == null){
			System.err.println("Invalid directory path");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		else{
			LOG.info("Job Instance created successfully.Running Map/Reduce now");
			job.submit();			
			return job.waitForCompletion(true)?0:1;
		}
		
	}
	
	public static void main(String args[]) throws Exception{
		LOG.info("############### {} ################",APP_ID);
		int exitCode = ToolRunner.run(new WeatherFileRecorder(), args);
		System.exit(exitCode);	
	}
	
	private static Job createJob(Tool tool,Configuration conf,Path dirPath,Path seqFilePath) throws IOException{
		LOG.info("Setting up the job instance");
		Job job = Job.getInstance();
		job.setJarByClass(tool.getClass());
		
		LOG.info("Adding the local files to the input");
		FileSystem localFS = FileSystem.getLocal(conf);
		if(!localFS.exists(dirPath)){
			LOG.error("The directory specified does not exist");
			return null;
		}
		RemoteIterator<LocatedFileStatus> files = localFS.listFiles(dirPath, false);
		while(files.hasNext()){
			LocatedFileStatus file = files.next();
			LOG.info("Adding {} to list of inputs",file.getPath().getName());
			FileInputFormat.addInputPath(job, file.getPath());			
		}
		
		SequenceFileOutputFormat.setOutputPath(job, seqFilePath);
		
		job.setMapperClass(WeatherRecordFilterMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(WeatherKeyWritable.class);
		job.setOutputValueClass(WeatherRecordWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		return job;
	}
}
