package com.elianda.etest.file;

import org.joda.time.LocalDateTime;

import java.util.UUID;

/**
 * @author tony
 * @describe fileTest
 * @date 2019-08-03
 */
public class FileTest {
    public static void main(String[] args) {
        String outFilePath = "";
        String userId = "wx_5535752";
        LocalDateTime now = LocalDateTime.now();
        outFilePath += "/video/"+userId + "/" +
                now.getYear() + "/" +
                now.getMonthOfYear() + "/" +
                now.getDayOfMonth()+"/";
        String originalFilename = "abc.mp4";
        int splitSign = originalFilename.lastIndexOf(".");
        String papa = originalFilename.substring(splitSign);
        String newFileName = UUID.randomUUID().toString().substring(0,8) + papa;
//        System.out.println(newFileName);
//        System.out.println(outFilePath);
        System.out.println(outFilePath+newFileName);
    }
}
