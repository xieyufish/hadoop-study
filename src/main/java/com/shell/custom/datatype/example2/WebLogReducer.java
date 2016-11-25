package com.shell.custom.datatype.example2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebLogReducer extends Reducer<WebLog, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(WebLog webLog, Iterable<IntWritable> values,
			Reducer<WebLog, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(webLog.getIp(), new IntWritable(sum));
	}

}
