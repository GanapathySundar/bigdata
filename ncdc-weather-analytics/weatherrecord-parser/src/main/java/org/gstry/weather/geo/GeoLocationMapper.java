package org.gstry.weather.geo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gstry.weather.writable.GeoPositionWritable;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.gstry.weather.writable.WeatherRecordWritable;

public class GeoLocationMapper extends Mapper<WeatherKeyWritable,WeatherRecordWritable,Text,GeoPositionWritable>{

	public void map(WeatherKeyWritable key,WeatherRecordWritable value,Context context) throws IOException, InterruptedException{		
		Text stationId = key.getStationId();
		GeoPositionWritable geoLocation = value.getLocation();
		context.write(stationId, geoLocation);
	}

}
