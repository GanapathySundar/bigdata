package org.gstry.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gstry.weather.geo.GeoLocationMapper;
import org.gstry.weather.geo.GeoLocationReducer;
import org.gstry.weather.geo.GeolocationCountReducer;
import org.gstry.weather.writable.GeoPositionWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationRecorder extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(LocationRecorder.class);
	private static final String APP_ID = "LOCATION RECORDER";
	private static final String LOC_FILE = "locations";
	private static final String COUNT_FILE = "counts";
	private static final int NO_REDUCE_TASKS = 5;
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2){
			System.err.printf("Usage : %s <inputDir> <outputDir>",getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		
		LOG.info("Setting up the geolocation job");
		Job locationJob = Job.getInstance(conf);
		locationJob.setJarByClass(getClass());
		locationJob.setInputFormatClass(SequenceFileInputFormat.class);
		locationJob.setMapperClass(GeoLocationMapper.class);
		locationJob.setReducerClass(GeoLocationReducer.class);
		locationJob.setOutputKeyClass(Text.class);
		locationJob.setOutputValueClass(GeoPositionWritable.class);
		locationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		SequenceFileInputFormat.addInputPath(locationJob, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(locationJob, new Path(args[1]+"/"+LOC_FILE));
		
		LOG.info("Setting up the records count job");
		/*Job recordCountJob = Job.getInstance(conf);
		recordCountJob.setJarByClass(getClass());
		recordCountJob.setInputFormatClass(SequenceFileInputFormat.class);
		recordCountJob.setMapperClass(GeoLocationMapper.class);
		recordCountJob.setMapOutputKeyClass(Text.class);		
		recordCountJob.setMapOutputValueClass(GeoPositionWritable.class);
		recordCountJob.setReducerClass(GeolocationCountReducer.class);
		recordCountJob.setNumReduceTasks(NO_REDUCE_TASKS);
		recordCountJob.setOutputKeyClass(Text.class);
		recordCountJob.setOutputValueClass(LongWritable.class);
		recordCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileInputFormat.addInputPath(recordCountJob, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(recordCountJob, new Path(args[1]+"/"+COUNT_FILE));*/
		
		LOG.info("Submitting the jobs");		
		//locationJob.submit();
		int res = locationJob.waitForCompletion(true)?0:1;
		return res;
	}
	
	public static void main(String[] args){
		LOG.info("################# {} ########################",APP_ID);
		int exitCode = -1;
		try {
			 exitCode = ToolRunner.run(new LocationRecorder(), args);
		} catch (Exception e) {
			LOG.error("Could not run execution:",e);
		}finally{
			System.exit(exitCode);
		}		
	}
	
	
	
	
}
