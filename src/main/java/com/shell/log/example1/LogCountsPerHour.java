package com.shell.log.example1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.shell.count.WordCount;

public class LogCountsPerHour extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LogCountsPerHour(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", WordCount.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName("LogCountsPerHour");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(LogMapper.class);
		job.setMapOutputKeyClass(DateWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(LogReducer.class);
		
		job.setOutputKeyClass(DateWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class LogMapper extends Mapper<LongWritable, Text, DateWritable, IntWritable> {
		private DateWritable date = new DateWritable();
		private IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, DateWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String text = value.toString();
			
			int openBracket = text.indexOf('[');
			int closeBracket = text.indexOf(']');
			if (openBracket != -1 && closeBracket != -1) {
				String dateString = text.substring(openBracket + 1, closeBracket);
				String dateTimeString = dateString.substring(0, dateString.length() - 6);
				try {
					Date dateTime = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH).parse(dateTimeString.trim());
					Calendar calendar = Calendar.getInstance(Locale.ENGLISH);
					calendar.setTime(dateTime);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					
					date.setDate(calendar.getTime());
					context.write(date, one);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class LogReducer extends Reducer<DateWritable, IntWritable, DateWritable, LongWritable> {
		@Override
		protected void reduce(DateWritable key, Iterable<IntWritable> values,
				Reducer<DateWritable, IntWritable, DateWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0 ;
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			context.write(key, new LongWritable(sum));
		}
	}
}
