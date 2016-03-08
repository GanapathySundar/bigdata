package org.gstry.weather.parser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.gstry.weather.writable.WeatherRecordWritable;

public class WeatherRecordFilterMapper extends Mapper<LongWritable, Text, LongWritable, WeatherRecordWritable> {
	
	private static final Double BAD_VALUE=-1000.0;
	private enum TemperatureQuality{
		BAD_TEMP_VALUE;
	}
	public void map(LongWritable lineNo,Text line,Context context){
		try {
			WeatherRecordWritable record = WeatherRecordParser.parseRecord(line.toString());
			if(record.getValue().get() == BAD_VALUE){
				context.getCounter(TemperatureQuality.BAD_TEMP_VALUE).increment(1);
			}
			else{
				context.write(lineNo, record);
			}
		} catch (Exception e) {			
			e.printStackTrace();
		}
			
	}
}
