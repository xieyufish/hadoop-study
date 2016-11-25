package com.shell.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StandaloneApp implements Tool {

	@Override
	public void setConf(Configuration conf) {
		
	}

	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", WordCount.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		Job job = Job.getInstance(new Configuration(), "wordcountApp");
		job.setJarByClass(StandaloneApp.class);
		
		job.setMapperClass(StandaloneMapper.class);
		job.setReducerClass(StandaloneReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new StandaloneApp(), args);
		System.exit(result);
	}
}
