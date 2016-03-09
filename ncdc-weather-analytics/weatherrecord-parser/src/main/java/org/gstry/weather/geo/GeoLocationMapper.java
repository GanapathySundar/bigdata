package org.gstry.weather.geo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gstry.weather.writable.GeoPositionWritable;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.gstry.weather.writable.WeatherRecordWritable;

public class GeoLocationMapper extends Mapper<WeatherKeyWritable,WeatherRecordWritable,Text,GeoPositionWritable>{
	

}
