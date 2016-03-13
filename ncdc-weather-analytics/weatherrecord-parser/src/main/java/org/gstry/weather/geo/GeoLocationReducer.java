package org.gstry.weather.geo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.gstry.weather.writable.GeoPositionWritable;

public class GeoLocationReducer extends Reducer<Text,GeoPositionWritable,Text,GeoPositionWritable> {
	
	public void reduce(Text key,Iterable<GeoPositionWritable>values,Context context) throws IOException, InterruptedException{		
		Iterator<GeoPositionWritable> itr = values.iterator();
		GeoPositionWritable position = itr.next();
		context.write(key, position);
	}

}
