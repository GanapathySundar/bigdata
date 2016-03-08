package org.gstry.weather.daily;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.gstry.weather.writable.WeatherKeyWritable;

public class DailyAggregatedWeatherReducer extends Reducer<WeatherKeyWritable,DoubleWritable,WeatherKeyWritable,DoubleWritable>{
	
	private static final double ABSOLUTE_ZERO = -273.0;

	@Override
	protected void reduce(WeatherKeyWritable key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {
		Iterator<DoubleWritable> itr = values.iterator();
		double maxTemp = ABSOLUTE_ZERO;
		while(itr.hasNext()){
			double temp = itr.next().get();
			if( temp > maxTemp){
				maxTemp = temp;
			}
		}
		context.write(key, new DoubleWritable(maxTemp));
	}
	
	

}
