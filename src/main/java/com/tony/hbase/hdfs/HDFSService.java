package com.tony.hbase.hdfs;

import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

/**
 * @author tony
 * @describe HDFSService
 * @date 2019-08-03
 */
public interface HDFSService {
    String uploadFile(MultipartFile file, String type) throws IOException, InterruptedException;

    String downloadFile(String filePath) throws IOException, InterruptedException;
}
