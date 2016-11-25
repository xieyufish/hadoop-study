package com.shell.base;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.AccessControlException;

public class ListDirectoryContents {
	
	public static void main(String[] args) throws URISyntaxException, AccessControlException, FileNotFoundException, IllegalArgumentException, IOException {
		Configuration conf = new HdfsConfiguration();
		
		FileContext fileContext = FileContext.getFileContext(new URI("hdfs://192.168.146.146:9000"), conf);
		
		RemoteIterator<FileStatus> fileStatusList = fileContext.listStatus(new Path("hdfs://192.168.146.146:9000/user/Administrator"));
		while(fileStatusList.hasNext()) {
			FileStatus fileStatus = fileStatusList.next();
			Path path = fileStatus.getPath();
			System.out.println(path);
		}
	}
}
