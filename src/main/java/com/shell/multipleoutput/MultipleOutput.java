package com.shell.multipleoutput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.shell.count.WordCount;

public class MultipleOutput {
	static String fruitOutputName = "fruit";
	static String colorOutputName = "color";
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\t");
			context.write(new Text(tokens[0]), value);
		}
	}
	
	static class MyReducer extends Reducer<Text, Text, Text, Writable> {
		MultipleOutputs<Text, Writable> mos;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Writable>.Context context)
				throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Writable>(context);
		}
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Writable>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String str = value.toString();
				String[] items = str.split("\t");
				
				mos.write(fruitOutputName, NullWritable.get(), new Text(items[1]), "fruit");
				mos.write(colorOutputName, NullWritable.get(), new Text(items[2]), "color");
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Writable>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", WordCount.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(MultipleOutput.class);
		job.setJobName("MultipleOutput");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);  // 可以避免part结果文件的产生
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleOutputs.addNamedOutput(job, fruitOutputName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, colorOutputName, TextOutputFormat.class, NullWritable.class, Text.class);
		
		job.waitForCompletion(true);
		
	}
}
