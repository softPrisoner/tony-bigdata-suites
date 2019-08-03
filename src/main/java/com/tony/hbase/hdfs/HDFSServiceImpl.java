package com.tony.hbase.hdfs;

import com.tony.hbase.contants.FilePrefixConstants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.LocalDateTime;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author tony
 * @describe HDFSServiceImpl
 * @date 2019-08-03
 */
@Service
public class HDFSServiceImpl implements HDFSService {
    @Override
    public String uploadFile(MultipartFile file, String type) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        String outFilePath = "";
        if ("video".equals(type)) {
            outFilePath += FilePrefixConstants.VEDIO_TYPE;
        }
        if ("audio".equals(type)) {
            outFilePath += FilePrefixConstants.AUDIO_TYPE;
        }
        String userId = "wx_5535752";
        LocalDateTime now = LocalDateTime.now();
        outFilePath += userId + "/" +
                now.getYear() + "/" +
                now.getMonthOfYear() + "/" +
                now.getDayOfMonth() + "/";

        String originalFilename = file.getOriginalFilename();
        int splitSign = originalFilename.lastIndexOf(".");
        String papa = originalFilename.substring(splitSign);
        String newFileName = UUID.randomUUID().toString().replace("-", "").substring(8) + papa;
        if (!fs.exists(new Path(outFilePath))) {
            //如果不存在目录,创建目录
            fs.mkdirs(new Path(outFilePath));
        }
        //拼接文件存储路径及名称
        outFilePath += newFileName;
        FSDataOutputStream outputStream = fs.create(new Path(outFilePath));
//        FileInputStream fileInputStream =new FileInputStream();
        IOUtils.copy(file.getInputStream(), outputStream);
        return outFilePath;
    }

    @Override
    public void downloadFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(filePath));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/home/tony/video/abc.mp4"));
        IOUtils.copy(in, fileOutputStream);
    }
}
