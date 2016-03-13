package org.gstry.weather.yearly;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;

public class YearlyAggregatedWeatherReducer extends Reducer<WeatherKeyWritable, TemperatureTuple, Text, Text> {
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
	@Override
	protected void reduce(WeatherKeyWritable key, Iterable<TemperatureTuple> values,Context context)
			throws IOException, InterruptedException {
		double minTemp=0;
		double maxTemp = 0;
		int noRecords = 0;
		Iterator<TemperatureTuple> itr = values.iterator();
		while(itr.hasNext()){
			TemperatureTuple value = itr.next();
			minTemp += value.getMinTemperature().get();
			maxTemp += value.getMaxTemperature().get();
			noRecords++;
		}
		minTemp = minTemp/noRecords;
		maxTemp = maxTemp/noRecords;
		String stationId = key.getStationId().toString();
		String year = sdf.format(key.getTimestamp().get());
		String opValue = stationId + "\t"+minTemp+"\t"+maxTemp;
		context.write(new Text(year), new Text(opValue));		
	}
	
	

}
