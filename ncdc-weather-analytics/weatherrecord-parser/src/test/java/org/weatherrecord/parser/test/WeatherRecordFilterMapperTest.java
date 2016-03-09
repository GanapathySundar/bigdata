package org.weatherrecord.parser.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.gstry.weather.parser.WeatherRecordFilterMapper;
import org.gstry.weather.writable.WeatherKeyWritable;
import org.gstry.weather.writable.WeatherRecordWritable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherRecordFilterMapperTest {

	private static final String TEST_RECORD = "0067011990999991950051507004+68750+023550FM-12+038299999V0203301N00671220001CN9999999N9+00001+99999999999";
	private static final Logger LOG = LoggerFactory.getLogger(WeatherRecordFilterMapperTest.class);
	
	
	@Test
	public void testWeatherRecordFilterMap() throws IOException{
		LOG.info("########### JUNIT TEST : FILTER AND PARSE WEATHER RECORDS ############");
		Pair<LongWritable,Text> ipRecord = new Pair<LongWritable,Text>(new LongWritable(1),new Text(TEST_RECORD));
		MapDriver<LongWritable,Text,WeatherKeyWritable,WeatherRecordWritable> mapDriver = 
				new MapDriver<LongWritable,Text,WeatherKeyWritable,WeatherRecordWritable>()
					.withMapper(new WeatherRecordFilterMapper())
					.withInput(ipRecord);
		
		List<Pair<WeatherKeyWritable,WeatherRecordWritable>> filteredRecords = mapDriver.run();
		LOG.info("Output Records:{}",filteredRecords.toString());					
	}
}
