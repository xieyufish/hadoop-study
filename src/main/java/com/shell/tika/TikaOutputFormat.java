package com.shell.tika;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TikaOutputFormat extends FileOutputFormat<Text, Text> {

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Path path = FileOutputFormat.getOutputPath(job);
		Path fullpath = new Path(path, "Srini.txt");
		FileSystem fs = path.getFileSystem(job.getConfiguration());
		FSDataOutputStream output = fs.create(fullpath, job);
		return new TikaRecordWriter(output);
	}

}
