package org.gstry.weather.yearly;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gstry.weather.writable.DateWritable;
import org.gstry.weather.writable.TemperatureTuple;
import org.gstry.weather.writable.WeatherKeyWritable;

public class YearlyAggregatedWeatherMapper extends Mapper<WeatherKeyWritable,TemperatureTuple,WeatherKeyWritable,TemperatureTuple> {

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
	
	public void map(WeatherKeyWritable key,TemperatureTuple value,Context context) throws IOException, InterruptedException{
		String year = sdf.format(key.getTimestamp().get());
		try {
			key.setTimestamp(new DateWritable(sdf.parse(year)));
			context.write(key, value);
			} catch (ParseException e) {			
			e.printStackTrace();
		}
		
	}
}
