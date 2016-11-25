package com.shell.custom.datatype.example2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapper extends Mapper<LongWritable, Text, WebLog, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WebLog, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split("\t");
		
		WebLog webLog = new WebLog(words[1], words[4], words[3], words[2], Integer.parseInt(words[0]));
		context.write(webLog, new IntWritable(1));
	}

}
