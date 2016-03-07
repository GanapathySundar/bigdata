package org.gstry.weather.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class WeatherRecordWritable implements WritableComparable<WeatherRecordWritable> {
	
	private DateWritable timestamp;
	private GeoPositionWritable location;
	private DoubleWritable value;
	
	
	public WeatherRecordWritable() {
		super();
		timestamp = new DateWritable();
		location = new GeoPositionWritable();
		value = new DoubleWritable();
	}
	
	
	public DateWritable getTimestamp() {
		return timestamp;
	}


	public void setTimestamp(DateWritable timestamp) {
		this.timestamp = timestamp;
	}


	public GeoPositionWritable getLocation() {
		return location;
	}


	public void setLocation(GeoPositionWritable location) {
		this.location = location;
	}


	public DoubleWritable getValue() {
		return value;
	}


	public void setValue(DoubleWritable value) {
		this.value = value;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		timestamp.readFields(in);
		location.readFields(in);
		value.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		timestamp.write(out);
		location.write(out);
		value.write(out);		
	}
	
	@Override
	public int compareTo(WeatherRecordWritable o) {
		int res = timestamp.compareTo(o.timestamp);
		if(res == 0){
			res = location.compareTo(o.location);
			if(res==0){
				res = value.compareTo(o.value);
			}
		}
		return res;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((location == null) ? 0 : location.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof WeatherRecordWritable)) {
			return false;
		}
		WeatherRecordWritable other = (WeatherRecordWritable) obj;
		if (location == null) {
			if (other.location != null) {
				return false;
			}
		} else if (!location.equals(other.location)) {
			return false;
		}
		if (timestamp == null) {
			if (other.timestamp != null) {
				return false;
			}
		} else if (!timestamp.equals(other.timestamp)) {
			return false;
		}
		if (value == null) {
			if (other.value != null) {
				return false;
			}
		} else if (!value.equals(other.value)) {
			return false;
		}
		return true;
	}
	
	
	
	
}
