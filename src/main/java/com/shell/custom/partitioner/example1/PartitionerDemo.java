package com.shell.custom.partitioner.example1;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionerDemo extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new PartitionerDemo(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName("PartitionerDemo");
		
		job.setMapperClass(PartitionerMap.class);
		job.setReducerClass(PartitionerReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(3);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(CustomPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	static class PartitionerMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (value.toString() != null) {
				String[] words = value.toString().split(" ");
				if (words.length == 2) {
					context.write(new Text(words[0]), new Text(words[1]));
				}
			}
		}
	}
	
	static class PartitionerReduce extends Reducer<Text, Text, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int gameCount=0;
			
			for(Text val : values) {
				gameCount++;
				val.toString();
			}
			context.write(new Text(key),new IntWritable(gameCount));
		}
	}
	
	static class CustomPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			if (numPartitions == 0) {
				return 0;
			}
			
			if (key.equals(new Text("Cricket"))) {
				return 0;
			}
			
			if (key.equals(new Text("Football"))) {
				return 1;
			}
			
			return 2;
		}
		
	}

}
