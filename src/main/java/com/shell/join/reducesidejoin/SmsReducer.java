package com.shell.join.reducesidejoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class SmsReducer extends Reducer<Text, Text, Text, Text> {
	
	private HashMap<String, String> deliverCodesMap = new HashMap<>(); 
	private enum MYCOUNTER {
		RECORD_COUNT,
		FILE_EXISTS,
		FILE_NOT_FOUND,
		SOME_OTHER_ERROR
	}
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		Path[] cacheFiles = Job.getInstance(context.getConfiguration()).getLocalCacheFiles();
		
		for (Path cacheFile : cacheFiles) {
			if (cacheFile.getName().trim().equals("DeliveryStatusCodes.txt")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDeliverStatusCodes(cacheFile, context);
			}
		}
	}
	
	private void loadDeliverStatusCodes(Path cacheFile, Context context) {
		BufferedReader bufferedReader = null;
		
		try {
			bufferedReader = new BufferedReader(new FileReader(cacheFile.toString()));
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				String[] splitArray = line.split(",");
				deliverCodesMap.put(splitArray[0], splitArray[1]);
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
					e.printStackTrace();
				} finally {
					bufferedReader = null;
				}
			}
		}
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String userName = null;
		String deliverReport = null;
		for (Text value : values) {
			String splitArray[] = value.toString().split("~");
			
			if (splitArray[0].equals("CD")) {
				userName = splitArray[1];
			} else if (splitArray[0].equals("DR")) {
				deliverReport = deliverCodesMap.get(splitArray[1]);
			}
		}
		
		if (userName != null && deliverReport != null) {
			context.write(new Text(userName), new Text(deliverReport));
		} else if (userName == null) {
			context.write(new Text("userName"), new Text(deliverReport));
		} else if (deliverReport == null) {
			context.write(new Text(userName), new Text("deliverReport"));
		}
	}
}
