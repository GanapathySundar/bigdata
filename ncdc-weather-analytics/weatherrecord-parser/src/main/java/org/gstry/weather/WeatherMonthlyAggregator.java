package org.gstry.weather;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gstry.weather.monthly.MonthlyAggregatedWeatherMapper;
import org.gstry.weather.monthly.MonthlyAggregatedWeatherReducer;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherMonthlyAggregator extends Configured implements Tool {
	
	private static final Logger LOG = LoggerFactory.getLogger(WeatherMonthlyAggregator.class);
	private static final String APP_ID = " MONTHLY MIN MAX TEMPERATURE AGGREGATOR";

	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length != 2){
			System.err.printf("Usage :%s [generic options] <ip_file> <op_file>",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Path ipFile = new Path(args[0]);
		Path opFile = new Path(args[1]);
		LOG.info("Setting up the job environment");
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(MonthlyAggregatedWeatherMapper.class);
		job.setReducerClass(MonthlyAggregatedWeatherReducer.class);
		job.setOutputKeyClass(WeatherKeyWritable.class);
		job.setOutputValueClass(TemperatureTuple.class);
		job.setNumReduceTasks(1);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		
		LOG.info("Starting the job");
		SequenceFileInputFormat.addInputPath(job, ipFile);
		SequenceFileOutputFormat.setOutputPath(job, opFile);
		
		return job.waitForCompletion(true)?0:1;
	}
	

	public static void main(String args[]) throws Exception{
		LOG.info("################ {} ########################",APP_ID);
		int status = ToolRunner.run(new WeatherMonthlyAggregator(), args);
		System.exit(status);
	}

}
