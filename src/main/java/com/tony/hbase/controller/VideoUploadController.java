package com.tony.hbase.controller;

import com.tony.hbase.hdfs.HDFSService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import java.io.IOException;


/**
 * @author tony
 * @describe VedioUploadController
 * @date 2019-08-03
 */
@Controller
@RequestMapping("v1/")
public class VideoUploadController {
    @Resource
    HDFSService hdfsService;
    private static final Logger LOGGER = LoggerFactory.getLogger(VideoUploadController.class);

    @RequestMapping("/page/upload")
    public String uploadPage() {
        return "VedioUpload.html";
    }

    @RequestMapping("/page/play")
    public String playPage() {
        return "VedioPlay.html";
    }

    @PostMapping("/video/upload")
    public String uploadVideo(@RequestParam String type, MultipartFile file) throws IOException {
        if (null == file || file.isEmpty()) {
            //前端可做路径校验
            return "文件不能为空";
        }
        String filePath = hdfsService.uploadFile(file, "video");
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.addObject("filePath", filePath);
        LOGGER.info("upload video success");
        return "redirect:/page/play";
    }

    @PutMapping("/audio/upload")
    public String uploadAudio(@RequestBody MultipartFile file) throws IOException {
        if (null == file || file.isEmpty()) {
            //前端可做路径校验
            return "文件不能为空";
        }
        hdfsService.uploadFile(file, "audio");
        LOGGER.info("upload audio success");
        return "上传音频成功";
    }
}
