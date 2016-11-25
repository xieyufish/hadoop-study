package com.shell.count;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StandaloneReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
	
	@Override
	protected void reduce(Text word, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		Iterator<IntWritable> itr = values.iterator();
		while(itr.hasNext()) {
			sum += itr.next().get();
		}
		context.write(word, new LongWritable(sum));

	}
}
