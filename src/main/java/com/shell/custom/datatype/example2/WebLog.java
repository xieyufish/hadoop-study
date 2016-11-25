package com.shell.custom.datatype.example2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WebLog implements WritableComparable<WebLog> {
	private Text siteUrl;
	private Text ip;
	private Text timestamp;
	private Text reqDate;
	private IntWritable reqNo;
	
	public WebLog() {
		siteUrl = new Text();
		ip = new Text();
		timestamp = new Text();
		reqDate = new Text();
		reqNo = new IntWritable();
	}

	public WebLog(String siteUrl, String ip, String timestamp, String reqDate, int reqNo) {
		this.siteUrl = new Text(siteUrl);
		this.ip = new Text(ip);
		this.timestamp = new Text(timestamp);
		this.reqDate = new Text(reqDate);
		this.reqNo = new IntWritable(reqNo);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		siteUrl.write(out);
		ip.write(out);
		timestamp.write(out);
		reqDate.write(out);
		reqNo.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		siteUrl.readFields(in);
		ip.readFields(in);
		timestamp.readFields(in);
		reqDate.readFields(in);
		reqNo.readFields(in);
	}

	public Text getSiteUrl() {
		return siteUrl;
	}

	public void setSiteUrl(Text siteUrl) {
		this.siteUrl = siteUrl;
	}

	public Text getIp() {
		return ip;
	}

	public void setIp(Text ip) {
		this.ip = ip;
	}

	public Text getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Text timestamp) {
		this.timestamp = timestamp;
	}

	public Text getReqDate() {
		return reqDate;
	}

	public void setReqDate(Text reqDate) {
		this.reqDate = reqDate;
	}

	public IntWritable getReqNo() {
		return reqNo;
	}

	public void setReqNo(IntWritable reqNo) {
		this.reqNo = reqNo;
	}

	@Override
	public int compareTo(WebLog o) {
		if (ip.compareTo(o.ip) == 0) {
			return timestamp.compareTo(o.timestamp);
		} else {
			return ip.compareTo(o.ip);
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof WebLog) {
			WebLog other = (WebLog) obj;
			return ip.equals(other.ip) && timestamp.equals(other.timestamp);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return ip.hashCode();
	}
}
