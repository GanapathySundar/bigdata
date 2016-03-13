package org.gstry.weather;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.gstry.weather.yearly.YearlyAggregatedWeatherMapper;
import org.gstry.weather.yearly.YearlyAggregatedWeatherReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class WeatherYearlyAggregator extends Configured implements Tool {
	
	private static final Logger LOG = LoggerFactory.getLogger(WeatherYearlyAggregator.class);
	private static final String APP_ID = "YEARLY WEATHER AGGREGATOR";
	
	@Override
	public int run(String[] args) throws Exception {
		int res=0;
		if(args.length != 2){
			System.err.printf("Usage :%s [generic options] <ip_file> <op_file>",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		
		LOG.info("Setting up the job");
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(YearlyAggregatedWeatherMapper.class);
		job.setMapOutputKeyClass(WeatherKeyWritable.class);
		job.setMapOutputValueClass(TemperatureTuple.class);
		job.setReducerClass(YearlyAggregatedWeatherReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		SequenceFileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		LOG.info("################## {} ######################",APP_ID);
		int status = ToolRunner.run(new WeatherYearlyAggregator(), args);
		System.exit(status);

	}

}
