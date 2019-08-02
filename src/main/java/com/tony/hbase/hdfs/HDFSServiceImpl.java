package com.tony.hbase.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author tony
 * @describe HDFSServiceImpl
 * @date 2019-08-03
 */
@Service
public class HDFSServiceImpl implements HDFSService {
    @Override
    public void uploadFile(String inputFilePath, String outFilePath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(new Path(outFilePath));
        FileInputStream fileInputStream = new FileInputStream(inputFilePath);
        IOUtils.copy(fileInputStream, outputStream);
    }
}
