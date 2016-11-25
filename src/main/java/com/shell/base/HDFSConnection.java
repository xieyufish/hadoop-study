package com.shell.base;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hdfs.HdfsConfiguration;

public class HDFSConnection {
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new HdfsConfiguration();
		FileContext fileContext = FileContext.getFileContext(new URI("hdfs://192.168.146.146:9000"), conf);
		
		//FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.146.146:9000"), conf);
//		if (fileSystem instanceof DistributedFileSystem) {
//			System.out.println("Successfully Connected to HDFS filesystem");
//		} else {
//			System.out.println("Other type of file system " + fileSystem.getClass());
//		}
		
		System.out.println(fileContext.getHomeDirectory());
	}
}
