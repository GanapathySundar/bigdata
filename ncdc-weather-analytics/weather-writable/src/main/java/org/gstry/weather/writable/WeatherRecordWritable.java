package org.gstry.weather.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class WeatherRecordWritable implements WritableComparable<WeatherRecordWritable> {
	
	
	private GeoPositionWritable location;
	private DoubleWritable value;
	
	
	public WeatherRecordWritable() {
		super();		
		location = new GeoPositionWritable();
		value = new DoubleWritable();
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
		location.readFields(in);
		value.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		location.write(out);
		value.write(out);		
	}
	
	@Override
	public int compareTo(WeatherRecordWritable o) {
		int res = location.compareTo(o.location);
		if(res==0){
			res = value.compareTo(o.value);
		}		
		return res;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((location == null) ? 0 : location.hashCode());		
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
		if (value == null) {
			if (other.value != null) {
				return false;
			}
		} else if (!value.equals(other.value)) {
			return false;
		}
		return true;
	}


	@Override
	public String toString() {
		return "WeatherRecordWritable [location=" + location
				+ ", value=" + value + "]";
	}
	
	
	
	
}
