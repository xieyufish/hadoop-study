package com.shell.log.example3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogProcessorMapper extends Mapper<LongWritable, Text, Text, MultiValueWritable> {
	private Text userHostText = new Text();
	private MultiValueWritable requestText = new MultiValueWritable();
	private MultiValueWritable bytesWritable = new MultiValueWritable();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, MultiValueWritable>.Context context)
			throws IOException, InterruptedException {
		String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";
		
		Pattern pattern = Pattern.compile(logEntryPattern);
		Matcher matcher = pattern.matcher(value.toString());
		if (!matcher.matches()) {
			return;
		}
		
		String userHost = matcher.group(1);
		userHostText.set(userHost);
		
		String request = matcher.group(5);
		requestText.set(new Text(request));
		
		int bytes = Integer.parseInt(matcher.group(7));
		bytesWritable.set(new IntWritable(bytes));
		
		// 一个key,输出两个不同类型的值, Text类型的request值和Int类型的bytes值
		context.write(userHostText, requestText);
		context.write(userHostText, bytesWritable);
	}

}
