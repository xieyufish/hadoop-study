package com.shell.tika;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

public class TikaRecordReader extends RecordReader<Text, Text> {
	private Text key = new Text();
	private Text value = new Text();
	private FileSplit fileSplit;
	@SuppressWarnings("unused")
	private Configuration conf;
	private boolean processed = false;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			Path path = fileSplit.getPath();
			key.set(path.toString());
			
			try {
				URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
				String con = new Tika().parseToString(new URL(path.toString()));
				String string = con.replaceAll("[$%&+,:;=?#|']", " ");
				String string2 = string.replaceAll("\\s+", " ");
				String low = string2.toLowerCase();
				value.set(low);
			} catch (TikaException e) {
				e.printStackTrace();
			}
			
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		
	}

}	
