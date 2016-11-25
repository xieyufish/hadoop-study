package com.shell.count;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {
	
	static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		 @Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			 StringTokenizer tokenizer = new StringTokenizer(value.toString());
			 while (tokenizer.hasMoreTokens()) {
				 String word = tokenizer.nextToken();
				 context.write(new Text(word), new IntWritable(1));
			 }
		}
	}
	
	static class CountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text word, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			Iterator<IntWritable> itr = values.iterator();
			while (itr.hasNext()) {
				int value = itr.next().get();
				sum += value;
			}
			
			context.write(word, new LongWritable(sum));
		}
		
		
	}
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", WordCount.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount");
		
		job.setJarByClass(WordCount.class);  // 必须指定, 否则报找不到类
//		job.setJobName("wordcount");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 这里设置的是map任务的输出结果格式, 而不是整个job的输入格式, 因为整个job的输入是有reduce任务的输出决定的
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
