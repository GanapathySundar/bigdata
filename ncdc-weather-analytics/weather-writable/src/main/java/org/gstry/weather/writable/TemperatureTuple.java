package org.gstry.weather.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class TemperatureTuple implements WritableComparable<TemperatureTuple> {
	
	private DoubleWritable maxTemperature;
	private DoubleWritable minTemperature;

	public TemperatureTuple() {
		super();
		maxTemperature = new DoubleWritable();
		minTemperature = new DoubleWritable();
	}
	
	public TemperatureTuple(DoubleWritable maxTemperature, DoubleWritable minTemperature) {
		super();
		this.maxTemperature = maxTemperature;
		this.minTemperature = minTemperature;
	}
	
	public DoubleWritable getMaxTemperature() {
		return maxTemperature;
	}

	public void setMaxTemperature(DoubleWritable maxTemperature) {
		this.maxTemperature = maxTemperature;
	}

	public DoubleWritable getMinTemperature() {
		return minTemperature;
	}

	public void setMinTemperature(DoubleWritable minTemperature) {
		this.minTemperature = minTemperature;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		maxTemperature.readFields(in);
		minTemperature.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		maxTemperature.write(out);
		minTemperature.write(out);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((maxTemperature == null) ? 0 : maxTemperature.hashCode());
		result = prime * result + ((minTemperature == null) ? 0 : minTemperature.hashCode());
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
		if (!(obj instanceof TemperatureTuple)) {
			return false;
		}
		TemperatureTuple other = (TemperatureTuple) obj;
		if (maxTemperature == null) {
			if (other.maxTemperature != null) {
				return false;
			}
		} else if (!maxTemperature.equals(other.maxTemperature)) {
			return false;
		}
		if (minTemperature == null) {
			if (other.minTemperature != null) {
				return false;
			}
		} else if (!minTemperature.equals(other.minTemperature)) {
			return false;
		}
		return true;
	}

	@Override
	public int compareTo(TemperatureTuple o) {
		int res = o.maxTemperature.compareTo(maxTemperature);
		if(res == 0)
			res = o.minTemperature.compareTo(minTemperature);
		return res;
	}

	@Override
	public String toString() {
		return "TemperatureTuple [maxTemperature=" + maxTemperature + ", minTemperature=" + minTemperature + "]";
	}
	
	

}
