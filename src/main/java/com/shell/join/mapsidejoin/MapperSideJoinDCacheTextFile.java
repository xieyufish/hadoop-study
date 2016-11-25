package com.shell.join.mapsidejoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSideJoinDCacheTextFile extends Mapper<LongWritable, Text, Text, Text> {
	private HashMap<String, String> departmentMap = new HashMap<>();
	private enum MYCOUNTER {
		RECORD_COUNT,
		FILE_EXISTS,
		FILE_NOT_FOUND,
		SOME_OTHER_ERROR
	}
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Path[] cacheFiles = Job.getInstance(context.getConfiguration()).getLocalCacheFiles();
		for (Path cacheFile : cacheFiles) {
			System.out.println(cacheFile.toString());
			if (cacheFile.getName().toString().trim().equals("departments.txt")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDepartmentsHashMap(cacheFile, context);
			}
		}
	}
	
	private void loadDepartmentsHashMap(Path path, Context context) {
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(path.toString()));
			String line = null;
			while((line = bufferedReader.readLine()) != null) {
				System.out.println(line);
				String[] departmentArray = line.split("\t");
				System.out.println(Arrays.toString(departmentArray));
				departmentMap.put(departmentArray[0].trim(), departmentArray[1].trim());
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					
				} finally {
					bufferedReader = null;
				}
			}
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
		
		String employee = value.toString();
		String[] employeeArray = employee.split("\t");
		
		String deptId = employeeArray[6];
		System.out.println(departmentMap);
		String deptName = departmentMap.get(deptId);
		
		Text outputKey = new Text(employeeArray[0]);
		Text outputValue = new Text(employeeArray[1] + "\t"
				+ employeeArray[2] + "\t"
				+ employeeArray[3] + "\t"
				+ employeeArray[4] + "\t"
				+ employeeArray[5] + "\t"
				+ employeeArray[6] + "\t"
				+ deptName);
		context.write(outputKey, outputValue);
	}

}
