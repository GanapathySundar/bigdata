package org.gstry.weather;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherRecorder extends Configured implements Tool{

	private static final Logger LOG = LoggerFactory.getLogger(WeatherRecorder.class);

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
}