package org.gstry.weather.geo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.gstry.weather.writable.GeoPositionWritable;

public class GeolocationCountReducer extends Reducer<Text,GeoPositionWritable,Text,LongWritable>{

	public void reduce(Text key,Iterable<GeoPositionWritable> values,Context context) throws IOException, InterruptedException{
		Iterator<GeoPositionWritable> itr = values.iterator();
		int recordCount = 0;
		while(itr.hasNext()){
			recordCount++;
		}
		context.write(key, new LongWritable(recordCount));
	}
}
