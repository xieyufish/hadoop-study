package com.shell.log.example3;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MultiValueWritable extends GenericWritable {
	
	@SuppressWarnings("rawtypes")
	private static Class[] CLASSES = new Class[] {
			IntWritable.class,
			Text.class
	};

	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}
	
}
