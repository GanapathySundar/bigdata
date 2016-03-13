package org.gstry.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gstry.weather.daily.DailyAggregatedWeatherMapper;
import org.gstry.weather.daily.DailyAggregatedWeatherReducer;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherDailyAggregator extends Configured implements Tool {
	
	private static final Logger LOG = LoggerFactory.getLogger(WeatherDailyAggregator.class);
	private static final String APP_ID = "WEATHER DAILY AGGREGATOR";

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2){
			System.err.printf("Usage :%s [generic options] <ip_file> <op_file>",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Path ipFile = new Path(args[0]);
		Path opFile = new Path(args[1]);
		Job job = buildJob(this,getConf(),ipFile,opFile);
		job.submit();		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		LOG.info("#################### {} #######################",APP_ID);
		int exitCode = ToolRunner.run(new WeatherDailyAggregator(), args);
		System.exit(exitCode);
	}
	
	private static Job buildJob(Tool tool,Configuration conf,Path ipFile,Path opFile) throws IOException{
		LOG.info("Creating a job instance");
		Job job = Job.getInstance(conf);
		job.setJarByClass(tool.getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(DailyAggregatedWeatherMapper.class);
		job.setMapOutputKeyClass(WeatherKeyWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(DailyAggregatedWeatherReducer.class);
		job.setOutputKeyClass(WeatherKeyWritable.class);	
		job.setOutputValueClass(TemperatureTuple.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);
		
		SequenceFileInputFormat.addInputPath(job, ipFile);
		SequenceFileOutputFormat.setOutputPath(job, opFile);
		return job;
	}

}
