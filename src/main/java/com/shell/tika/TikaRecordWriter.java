package com.shell.tika;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TikaRecordWriter extends RecordWriter<Text, Text> {
	private DataOutputStream out;
	
	public TikaRecordWriter(DataOutputStream output) {
		this.out = output;
		try {
			out.writeBytes("result:\r\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, Text value) throws IOException, InterruptedException {
		out.writeBytes(key.toString());
		out.writeBytes(",");
		out.writeBytes(value.toString());
		out.writeBytes("\r\n");
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

}
