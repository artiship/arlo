package io.github.artiship.arlo.scheduler.worker.util;

import com.aliyun.oss.model.GetObjectRequest;

import java.io.File;


public class OssUtils {


    public static void downloadFileToLocal(OSSClientHolder ossClientHolder, String key, String targetPath) {
        ossClientHolder.getOssClient()
                       .getObject(new GetObjectRequest(ossClientHolder.getBucketName(), key),
                               new File(targetPath));
    }

}
