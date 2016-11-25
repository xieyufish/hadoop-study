package com.shell.custom.partitioner.example2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		logger.debug("Entering WordCountMapper.map() " + this);
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, " ");
		
		while(tokenizer.hasMoreTokens()) {
			context.write(new Text(tokenizer.nextToken()), new IntWritable(1));
		}
		
		logger.debug("Exiting WordCountMapper.map()");
	}

}
