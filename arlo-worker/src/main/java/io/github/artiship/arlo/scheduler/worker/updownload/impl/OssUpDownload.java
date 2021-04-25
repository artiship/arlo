package io.github.artiship.arlo.scheduler.worker.updownload.impl;

import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.ObjectMetadata;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.File;


@Slf4j
public class OssUpDownload extends AbstractUpDownload {

    //oss连接
    private OSSClientHolder ossClientHolder;

    //日志输入文件
    private String logFile;

    //上传OSS的元数据
    private ObjectMetadata meta = new ObjectMetadata();

    //正常日志append请求
    private AppendObjectRequest normalLogAppendObjectRequest;
    private AppendObjectResult normalLogAppendObjectResult;

    //
    public OssUpDownload(String logFile, OSSClientHolder ossClientHolder) {
        this.logFile = logFile;
        this.ossClientHolder = ossClientHolder;

        //普通文本
        meta.setContentType("text/plain");
    }

    @Override
    public void download(String sourcePath, String targetPath) throws Exception {
        ossClientHolder.listFiles(sourcePath)
                       .ifPresent(list -> list.forEach(objectSummary -> {
                           String sourceFilePath = objectSummary.getKey();
                           String filename = sourceFilePath.substring(sourceFilePath.lastIndexOf("/") + 1);

                           if (filename.indexOf(".") == -1) return;

                           String targetFilePath = new StringBuilder(targetPath).append(File.separator)
                                                                                .append(filename)
                                                                                .toString();

                           ossClientHolder.downloadFile(sourceFilePath, targetFilePath);
                       }));
    }

    @Override
    public void uploadLog(String logContext) {
        synchronized (this) {
            StringBuilder writeContextWithWrap = new StringBuilder(logContext);
            writeContextWithWrap.append("\n");

            if (null == normalLogAppendObjectRequest) {
                normalLogAppendObjectRequest = new AppendObjectRequest(ossClientHolder.getBucketName(), logFile,
                        new ByteArrayInputStream(writeContextWithWrap.toString()
                                                                     .getBytes()), meta);
                normalLogAppendObjectRequest.setPosition(0L);
            } else {
                normalLogAppendObjectRequest.setPosition(normalLogAppendObjectResult.getNextPosition());
                normalLogAppendObjectRequest.setInputStream(new ByteArrayInputStream(writeContextWithWrap.toString()
                                                                                                         .getBytes()));
            }

            normalLogAppendObjectResult = ossClientHolder.getOssClient()
                                                         .appendObject(normalLogAppendObjectRequest);
        }
    }


    @Override
    public void preprocess() {
        ossClientHolder.getOssClient()
                       .deleteObject(ossClientHolder.getBucketName(), logFile);
    }
}
