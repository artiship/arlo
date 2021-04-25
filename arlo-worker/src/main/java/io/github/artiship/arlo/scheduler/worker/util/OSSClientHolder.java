package io.github.artiship.arlo.scheduler.worker.util;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;


@Component
@Slf4j
public class OSSClientHolder implements InitializingBean, DisposableBean {

    @Value("${oss.url}")
    private String END_POINT;
    @Value("${oss.key}")
    private String ACCESS_KEY_ID;
    @Value("${oss.secret}")
    private String ACCESS_KEY_SECRET;
    @Value("${oss.qe.bucket.name}")
    private String QE_BUCKET_NAME;
    @Value("${oss.common.path:/arlo/common}")
    private String ossCommonPath;
    @Value("${oss.list.max.keys:500}")
    private int ossListMaxKeys;

    @Getter
    @Value("${arlo.worker.oss.common.objects}")
    private String commonObjects;

    @Getter
    private OSSClient ossClient;

    @Override
    public void afterPropertiesSet() throws Exception {
        log.debug("init oss client start.");
        ossClient = new OSSClient(END_POINT, new DefaultCredentialProvider(ACCESS_KEY_ID, ACCESS_KEY_SECRET), null);
        log.debug("init oss client success.");
    }

    @Override
    public void destroy() throws Exception {
        log.debug("stop oss client.");
        if (null != ossClient) {
            ossClient.shutdown();
        }
    }

    public String getBucketName() {
        return QE_BUCKET_NAME;
    }

    public String getCommonPath() {
        return this.ossCommonPath;
    }

    public Optional<List<OSSObjectSummary>> listFiles(String commonPath) {
        ObjectListing objects =
                ossClient.listObjects(new ListObjectsRequest(getBucketName()).withPrefix(commonPath)
                                                                             .withMaxKeys(ossListMaxKeys));
        if (objects == null) {
            log.info("Oss common path {} does not exist", commonPath);
            return empty();
        }

        return of(objects.getObjectSummaries());
    }

    public void downloadFile(String sourceFilePath, String targetFilePath) {
        log.info("Download oss file from {} to {}", sourceFilePath, targetFilePath);
        ossClient.getObject(new GetObjectRequest(getBucketName(), sourceFilePath), new File(targetFilePath));
    }
}
