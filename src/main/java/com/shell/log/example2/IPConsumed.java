package com.shell.log.example2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.shell.count.WordCount;

/**
 * 每个IP的访问资源量bytes,以及访问的url
 * @author Administrator
 *
 */
public class IPConsumed extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new IPConsumed(), args));
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
		job.setJobName("IPConsumed");
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(IPConsumedMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(IPConsumedReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class IPConsumedMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Pattern logPattern = Pattern.compile("([^\\s]+) - - \\[(.+)\\] \"([^\\s]+) (/[^\\s]*) HTTP/[^\\s]+\" [^\\s]+ ([0-9]+)");
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Matcher matcher = logPattern.matcher(value.toString());
			if (matcher.matches()) {
				String ip = matcher.group(1);
				String url_bytes = matcher.group(4) + " " + matcher.group(5);
				context.write(new Text(ip), new Text(url_bytes));
			}
		}
	}
	
	public static class IPConsumedReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		private Set<String> set = new HashSet<>();
		StringBuilder requests = new StringBuilder();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text value : values) {
				String urlByte = value.toString();
				String[] url_byte = urlByte.split(" ");
				String url = url_byte[0];
				String bytes = url_byte[1];
				if (!set.contains(url)) {
					set.add(url);
					requests.append(url).append("\t");
				}
				
				sum += Integer.parseInt(bytes);
			}
			
			result.set("Total Size : " + sum + "\t" + "URL's : " + requests);
			context.write(key, result);
		}
	}

}
