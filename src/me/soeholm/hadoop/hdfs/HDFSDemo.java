package me.soeholm.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HDFSDemo {

	/*
	 * Make a directory in HDFS
	 */
	@Test
	public void testMkdir() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
		boolean flag = fs.mkdirs(new Path("/test/child"));
		System.out.println(flag);
	}
	
	/*
	 * Upload a local file into HDFS
	 */
	@Test
	public void testUpload() throws Exception{
		InputStream in = new FileInputStream("/home/niels/ubuntu.todo");
		
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
		OutputStream out = fs.create(new Path("/test/child/my2.jdk"));
		
		IOUtils.copyBytes(in, out,4096,true);
	}
	
	/*
	 * Download a file from HDFS
	 */
	@Test
	public void testDownload() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
		fs.copyToLocalFile(false, new Path("/test/child/my.jdk"), new Path("/home/niels/hadoop/my.jdk"));
	}
	
	/*
	 * Delete a file from HDFS
	 */
	@Test
	public void testDeleteFile() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
		boolean flag = fs.delete(new Path("/test/child/my.jdk"), false);
		System.out.println(flag);
	}
	
	/*
	 * Remove a directory from HDFS
	 */
	@Test
	public void testRemoveDirectory() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
		boolean flag = fs.delete(new Path("/test/child"), true);
		System.out.println(flag);
	}
	
	/*
	 * List all the files and sub directory in a directory
	 */
	@Test
	public void testListDirectory() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
		RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("/test/child"), false);
		while(it.hasNext()) {
			LocatedFileStatus file = it.next();
			System.out.println(file.getPath());
		}
	}

	public static void main(String[] args) throws Exception {
		HDFSDemo demo = new HDFSDemo();
		demo.testListDirectory();
	}
}