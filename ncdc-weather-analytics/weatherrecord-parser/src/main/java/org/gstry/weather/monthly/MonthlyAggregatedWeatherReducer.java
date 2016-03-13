package org.gstry.weather.monthly;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;

public class MonthlyAggregatedWeatherReducer extends Reducer<WeatherKeyWritable,TemperatureTuple,WeatherKeyWritable,TemperatureTuple> {
	
	public void reduce(WeatherKeyWritable key,Iterable<TemperatureTuple> values,Context context) throws IOException, InterruptedException{
		double maxTemp = 0; double minTemp = 0;
		int noRecords = 0;
		Iterator<TemperatureTuple> itr = values.iterator();
		while(itr.hasNext()){
			TemperatureTuple value = itr.next();
			maxTemp += value.getMaxTemperature().get();
			minTemp += value.getMinTemperature().get();
			noRecords++;
		}
		maxTemp = maxTemp/noRecords;
		minTemp = minTemp/noRecords;
		TemperatureTuple opValue = new TemperatureTuple(new DoubleWritable(maxTemp),new DoubleWritable(minTemp));
		context.write(key, opValue);
	}

}
