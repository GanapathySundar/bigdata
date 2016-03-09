package org.gstry.weather.daily;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;

public class DailyAggregatedWeatherReducer extends Reducer<WeatherKeyWritable,DoubleWritable,WeatherKeyWritable,TemperatureTuple>{
	
	private static final double ABSOLUTE_ZERO = -273.0;
	private static final double ABSOLUTE_MAX = 100;

	@Override
	protected void reduce(WeatherKeyWritable key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {
		Iterator<DoubleWritable> itr = values.iterator();
		double maxTemp = ABSOLUTE_ZERO;
		double minTemp = ABSOLUTE_MAX;
		while(itr.hasNext()){
			double temp = itr.next().get();
			if( temp > maxTemp){
				maxTemp = temp;
			}
			if(temp < minTemp){
				minTemp = temp;
			}
		}
		context.write(key,new TemperatureTuple(new DoubleWritable(maxTemp),new DoubleWritable(minTemp)));
	}
	
	

}
