package com.shell.count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StandaloneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while(tokenizer.hasMoreElements()) {
			String word = tokenizer.nextToken();
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
