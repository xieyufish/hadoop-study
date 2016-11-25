package com.shell.base;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;

public class CopyFileToHDFS {
	public static void main(String[] args) throws URISyntaxException, AccessControlException, FileAlreadyExistsException, ParentNotDirectoryException, IllegalArgumentException, IOException {
		
		Configuration conf = new HdfsConfiguration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		InputStream inputStream = new BufferedInputStream(new FileInputStream("src/main/resources/wordcount.txt"));
		
		FileContext fileContext = FileContext.getFileContext(new URI("hdfs://192.168.146.146:9000"), conf);
		
		EnumSet<CreateFlag> createFlags = EnumSet.of(CreateFlag.CREATE);
		
		Path path = new Path("example_files/wordcount.txt");
		fileContext.deleteOnExit(path);
		FSDataOutputStream outputStream = fileContext.create(path, createFlags, 
				CreateOpts.createParent());
		
		IOUtils.copyBytes(inputStream, outputStream, 2048, true);
		
		
	}
}
