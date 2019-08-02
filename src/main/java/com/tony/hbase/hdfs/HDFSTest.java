package com.tony.hbase.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author tony
 * @describe HDFSTest
 * @date 2019-08-02
 */
public class HDFSTest {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        Path workingDir=fs.getWorkingDirectory();
        Path newPath=new Path("/test1/");
        System.setProperty("HADOOP_USER_NAME", "tony");
        System.setProperty("hadoop.home.dir", "/");
        if (!fs.exists(newPath)){
            fs.mkdirs(newPath);
        }
        FSDataOutputStream outputStream = fs.create(new Path("/test1/test.txt"));
    FileInputStream fileInputStream=new FileInputStream("/home/tony/spark/test.txt");
        IOUtils.copy(fileInputStream,outputStream);
    }
}
