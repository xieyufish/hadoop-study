package com.shell.log.example1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public class DateWritable implements WritableComparable<DateWritable> {
	private final static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd' T 'HH:mm:ss.SSS");
	private Date date;
	
	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(date.getTime());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = new Date(in.readLong());
	}

	@Override
	public int compareTo(DateWritable o) {
		return date.compareTo(o.getDate());
	}
	
	@Override
	public String toString() {
		return formatter.format(date);
	}
}
