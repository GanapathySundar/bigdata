package org.gstry.weather.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Writable class to represent a {@link Date} class as a Hadoop Serializable data type
 * @author Ganapathy Sundar
 *
 */
public class DateWritable implements WritableComparable<DateWritable> {
	
	private Date timestamp;
	
	/**
	 * Default Constructor
	 */
	public DateWritable() {
		super();		
		timestamp = Calendar.getInstance().getTime();
	}

	/**
	 * Constructor using a Date
	 * @param timestamp
	 */
	public DateWritable(Date timestamp) {
		super();
		this.timestamp = timestamp;
	}
	
	/**
	 * Get the timestamp value assosciated
	 * @return timestamp
	 */
	public Date get() {
		return timestamp;
	}

	
	public void set(Date timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		LongWritable timeLong = new LongWritable();
		timeLong.readFields(in);
		this.timestamp = new Date(timeLong.get());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		LongWritable timeLong = new LongWritable(timestamp.getTime());
		timeLong.write(out);
	}

	@Override
	public int compareTo(DateWritable o) {		
		return this.timestamp.compareTo(o.timestamp);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
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
		if (!(obj instanceof DateWritable)) {
			return false;
		}
		DateWritable other = (DateWritable) obj;
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
		return "DateWritable [timestamp=" + timestamp + "]";
	}
	
	

}
