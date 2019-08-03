package com.tony.hbase.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author tony
 * @describe HDFSDownLoadTest
 * @date 2019-08-03
 */
public class HDFSDownLoadTest {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        System.out.println("开始读取文件");
        FSDataInputStream in = fs.open(new Path("/video/wx_5535752/2019/8/3/89ed46b39eff915604ac94d3.mp4"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/home/tony/video/abc.mp4"));
        IOUtils.copy(in, fileOutputStream);
        System.out.println("视频文件写入完毕");
    }
}
