package com.shell.nlinereader;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		System.out.println("输出的key和value:" + " key=" + key + " value=" + value.toString());
		while(tokenizer.hasMoreTokens()) {
			context.progress();
			context.write(new Text(tokenizer.nextToken()), new IntWritable(1));
		}
	}

}
