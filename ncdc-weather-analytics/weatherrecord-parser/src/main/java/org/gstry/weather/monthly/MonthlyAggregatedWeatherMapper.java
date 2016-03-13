package org.gstry.weather.monthly;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.gstry.weather.writable.DateWritable;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;

public class MonthlyAggregatedWeatherMapper extends Mapper<WeatherKeyWritable,TemperatureTuple,WeatherKeyWritable,TemperatureTuple> {
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("MM-yyyy");
	
	
	public void map(WeatherKeyWritable key,TemperatureTuple value,Context context) throws IOException, InterruptedException{
		WeatherKeyWritable opKey = new WeatherKeyWritable();
		String monthString = sdf.format(key.getTimestamp().get());
		
		try {
			opKey.setTimestamp(new DateWritable(sdf.parse(monthString)));
			opKey.setStationId(key.getStationId());
		} catch (ParseException e) {			
			e.printStackTrace();
		}
		context.write(opKey, value);
	}

}
