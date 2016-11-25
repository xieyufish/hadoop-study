package com.shell.log.example3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class LogProcessorReducer extends Reducer<Text, MultiValueWritable, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<MultiValueWritable> values,
			Reducer<Text, MultiValueWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		StringBuilder requests = new StringBuilder();
		for (MultiValueWritable value : values) {
			Writable writable = value.get();
			if (writable instanceof IntWritable) {
				sum += ((IntWritable)writable).get();
			} else {
				requests.append(((Text)writable).toString());
				requests.append("\t");
			}
		}
		
		context.write(key, new Text(sum + "\t" + requests));
	}

}
