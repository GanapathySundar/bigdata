package org.gstry.weather.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class GeoPositionWritable implements WritableComparable<GeoPositionWritable> {

	private DoubleWritable latitude;
	private DoubleWritable longitude;
	
	
	public GeoPositionWritable() {
		super();
		latitude = new DoubleWritable(0);
		longitude = new DoubleWritable(0);
	}
	
	
	public GeoPositionWritable(DoubleWritable latitude, DoubleWritable longitude) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public DoubleWritable getLatitude() {
		return latitude;
	}

	public void setLatitude(DoubleWritable latitude) {
		this.latitude = latitude;
	}


	public DoubleWritable getLongitude() {
		return longitude;
	}


	public void setLongitude(DoubleWritable longitude) {
		this.longitude = longitude;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		latitude.readFields(in);
		longitude.readFields(in);		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		latitude.write(out);
		longitude.write(out);	
	}
	
	@Override
	public int compareTo(GeoPositionWritable o) {
		int res=o.latitude.compareTo(latitude);
		if(res == 0){
			res= longitude.compareTo(o.longitude);
		}
		return res;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((latitude == null) ? 0 : latitude.hashCode());
		result = prime * result + ((longitude == null) ? 0 : longitude.hashCode());
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
		if (!(obj instanceof GeoPositionWritable)) {
			return false;
		}
		GeoPositionWritable other = (GeoPositionWritable) obj;
		if (latitude == null) {
			if (other.latitude != null) {
				return false;
			}
		} else if (!latitude.equals(other.latitude)) {
			return false;
		}
		if (longitude == null) {
			if (other.longitude != null) {
				return false;
			}
		} else if (!longitude.equals(other.longitude)) {
			return false;
		}
		return true;
	}


	@Override
	public String toString() {
		return "GeoPositionWritable [latitude=" + latitude + ", longitude=" + longitude + "]";
	}
	
	
	
	
}
