package com.tony.hbase.hdfs;

import java.io.IOException;

/**
 * @author tony
 * @describe HDFSService
 * @date 2019-08-03
 */
public interface HDFSService {
   void uploadFile(String inputFilePath, String outFilePath) throws IOException;
}
