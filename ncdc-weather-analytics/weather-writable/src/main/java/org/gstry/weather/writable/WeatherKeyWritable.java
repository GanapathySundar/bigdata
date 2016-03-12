package org.gstry.weather.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WeatherKeyWritable implements WritableComparable<WeatherKeyWritable> {
	
	private Text stationId;
	private DateWritable timestamp;
	
	

	public WeatherKeyWritable() {
		super();
		this.stationId = new Text();
		this.timestamp = new DateWritable();
	}	

	public WeatherKeyWritable(Text stationId, DateWritable timestamp) {
		super();
		this.stationId = stationId;
		this.timestamp = timestamp;
	}
	
	public Text getStationId() {
		return stationId;
	}
	
	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}
	
	public DateWritable getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(DateWritable timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stationId.readFields(in);
		timestamp.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		stationId.write(out);
		timestamp.write(out);		
	}

	@Override
	public int compareTo(WeatherKeyWritable o) {
		int res = stationId.compareTo(o.stationId);
		if(res == 0){
			res = timestamp.compareTo(o.timestamp);
		}
		return res;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stationId == null) ? 0 : stationId.hashCode());
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
		if (!(obj instanceof WeatherKeyWritable)) {
			return false;
		}
		WeatherKeyWritable other = (WeatherKeyWritable) obj;
		if (stationId == null) {
			if (other.stationId != null) {
				return false;
			}
		} else if (!stationId.equals(other.stationId)) {
			return false;
		}
		if (timestamp == null) {
			if (other.timestamp != null) {
				return false;
			}
		} else if (!timestamp.equals(other.timestamp)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "WeatherKeyWritable [stationId=" + stationId + ", timestamp=" + timestamp + "]";
	}
	
	
	
	

}
