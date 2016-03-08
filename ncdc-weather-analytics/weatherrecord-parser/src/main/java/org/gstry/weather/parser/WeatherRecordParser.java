package org.gstry.weather.parser;

import java.text.SimpleDateFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.gstry.weather.writable.DateWritable;
import org.gstry.weather.writable.GeoPositionWritable;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.gstry.weather.writable.WeatherRecordWritable;

public abstract class WeatherRecordParser {

	private static final String INVALID_TEMPERATURE = "9999";
	private static final Double BAD_VALUE=-1000.0;
	public static WeatherRecordWritable parseRecord(String line) throws Exception{
		
		WeatherRecordWritable record = new WeatherRecordWritable();
				
		double latitude = 0.001*Double.parseDouble(line.charAt(28)=='+'?line.substring(29,34):line.substring(28,34));
		double longitude = 0.001*Double.parseDouble(line.charAt(34)=='+'?line.substring(35,41):line.substring(34,41));
		record.setLocation(new GeoPositionWritable(new DoubleWritable(latitude),new DoubleWritable(longitude)));
		
		String temp = line.substring(88, 92);
		String qc = line.substring(92,93);
		if(!temp.equalsIgnoreCase(INVALID_TEMPERATURE) && qc.matches("[01459]")){
			record.setValue(new DoubleWritable(0.01*Double.parseDouble(line.charAt(87)=='+'?line.substring(88,92):line.substring(87,92))));
		}else{
			record.setValue(new DoubleWritable(BAD_VALUE));
		}
		
		return record;
	}
	
	public static WeatherKeyWritable parseKey(String line)throws Exception{
		
		WeatherKeyWritable key = new WeatherKeyWritable();
		SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm");
		
		String timeString = line.substring(15,19)+"-"+line.substring(19,21)+"-"+line.substring(21,23)+" "
				   +line.substring(23,25)+":"+line.substring(25,27);
		key.setStationId(new Text(line.substring(4, 10)));
		key.setTimestamp(new DateWritable(format.parse(timeString)));
		
		return key;
	}
}
