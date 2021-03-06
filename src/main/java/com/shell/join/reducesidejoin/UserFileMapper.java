package com.shell.join.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserFileMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String cellNumber;
	private String userName;
	private String fileTag = "CD~";
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splitArray = line.split(",");
		cellNumber = splitArray[0];
		userName = splitArray[1];
		
		context.write(new Text(cellNumber), new Text(fileTag + userName));
	}

}
