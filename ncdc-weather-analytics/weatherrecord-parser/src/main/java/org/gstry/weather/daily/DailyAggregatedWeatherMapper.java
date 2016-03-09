package org.gstry.weather.daily;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.gstry.weather.writable.DateWritable;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.gstry.weather.writable.WeatherRecordWritable;

public class DailyAggregatedWeatherMapper extends Mapper<WeatherKeyWritable,WeatherRecordWritable,WeatherKeyWritable,DoubleWritable>{
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-YYYY");
	
	public void map(WeatherKeyWritable ipKey,WeatherRecordWritable record,Context context) throws IOException, InterruptedException{
		String ipDateString = sdf.format(ipKey.getTimestamp().get());
		WeatherKeyWritable opKey = new WeatherKeyWritable();
		
		try {
			opKey.setTimestamp(new DateWritable(sdf.parse(ipDateString)));
			opKey.setStationId(ipKey.getStationId());
			context.write(opKey, record.getValue());
		} catch (ParseException e) {			
			e.printStackTrace();
		}
	}

}
