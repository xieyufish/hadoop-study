package com.shell.custom.datatype.example1;

import java.io.IOException;

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

public class ModelTotalMileage extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ModelTotalMileage(), args));
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
		job.setJobName("ModelTotalMileage");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(ModelMileageMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Vehicle.class);
		
		job.setReducerClass(ModelTotalMileageReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	static class ModelMileageMapper extends Mapper<LongWritable, Text, Text, Vehicle> {
		private Vehicle vehicle;
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Vehicle>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] vehicleInfos = line.split(" ");
			if (vehicle == null) {
				vehicle = new Vehicle();
			}
			vehicle.setModel(vehicleInfos[0]);
			vehicle.setVin(vehicleInfos[1]);
			vehicle.setMileage(Integer.parseInt(vehicleInfos[2]));
			
			context.write(new Text(vehicleInfos[0]), vehicle);
		}
	}
	
	static class ModelTotalMileageReducer extends Reducer<Text, Vehicle, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<Vehicle> vehicles, Reducer<Text, Vehicle, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Vehicle vehicle : vehicles) {
				sum += vehicle.getMileage();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}
