package io.github.artiship.arlo.scheduler.worker.util;

import io.github.artiship.arlo.model.enums.FileType;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

import static java.io.File.separator;


@Slf4j
public class SyncCommonShellUtil {


    public synchronized static void syncAllCommonShell(OSSClientHolder ossClientHolder) {
        for (FileType fileType : FileType.values()) {
            syncCommonShell(ossClientHolder, fileType);
        }
    }


    private static void syncCommonShell(OSSClientHolder ossClientHolder, FileType fileType) {
        log.info("sync all shell, filetype : [{}]", fileType.getDir());
        String commonPath = new StringBuilder().append(ossClientHolder.getCommonPath())
                                               .append(separator)
                                               .append(fileType.getDir())
                                               .toString();

        ossClientHolder.listFiles(commonPath)
                       .ifPresent(list -> list.forEach(objectSummary -> {
                           if (notDirectory(objectSummary.getKey())) {
                               log.warn("sync all shell, unknown oss key : [{}]", objectSummary.getKey());
                               return;
                           }

                           update(
                                   ossClientHolder,
                                   objectSummary.getKey(),
                                   getFilename(objectSummary.getKey()),
                                   fileType,
                                   "sync-all"
                           );
                       }));
    }

    public static boolean notDirectory(String ossKey) {
        if (ossKey == null) return true;
        if (ossKey.indexOf(".") == -1) return true;
        return false;
    }


    private static void update(OSSClientHolder ossClientHolder, String ossFilePath, String fileName, FileType fileType, String taskTag) {
        //文件下载到本地的临时目录
        StringBuilder targetFilePath = new StringBuilder();
        targetFilePath.append(Constants.LOCAL_SYNC_TMP)
                      .append(File.separator)
                      .append(fileType.getDir())
                      .append(File.separator)
                      .append(fileName);

        //临时文件(OSS文件下载到临时目录)
        File tmpFile = new File(targetFilePath.toString());

        try {
            if (ossClientHolder.getOssClient()
                               .doesObjectExist(ossClientHolder.getBucketName(), ossFilePath)) {
                log.info("update file start, taskTag [{}]", taskTag);
                //如果临时文件本地存在, 先删除
                if (tmpFile.exists()) {
                    tmpFile.delete();
                }

                //把oss文件download本地临时目录
                OssUtils.downloadFileToLocal(ossClientHolder, ossFilePath, targetFilePath.toString());

                if (!tmpFile.exists()) {
                    log.error("update file failed, file download from oss not exist, oss path : [{}], local path : [{}], taskTag : [{}]",
                            ossFilePath, targetFilePath.toString(), taskTag);
                    return;
                }

                //本地实际存储目录对于的文件
                StringBuilder localTargetFileBuilder = new StringBuilder();
                localTargetFileBuilder.append(Constants.LOCAL_COMMON_SHELL_BASE_PATH)
                                      .append(File.separator)
                                      .append(fileType.getDir())
                                      .append(File.separator)
                                      .append(fileName);
                File localOldFile = new File(localTargetFileBuilder.toString());
                if (!localOldFile.exists()) {
                    //本地文件不存在, 直接将update操作转换成create操作
                    log.warn("update file, local file not exist, do create. filename : [{}], filetype : [{}], taskTag : [{}]"
                            , fileName, fileType.getDir(), taskTag);
                    create(ossClientHolder, ossFilePath, fileName, fileType, taskTag);
                    return;
                }

                //本地文件和oss上面的文件不一致, 那么使用oss上面的文件替换本地文件
                if (isChange(targetFilePath.toString(), localTargetFileBuilder.toString())) {
                    //删除旧的文件
                    FileUtils.deleteQuietly(localOldFile);
                    //将新文件更新到目标目录
                    try {
                        FileUtils.moveFile(tmpFile, localOldFile);
                        log.info("update file success, filename : [{}], filetype : [{}], taskTag : [{}]", fileName,
                                fileType.getDir(), taskTag);
                    } catch (IOException e) {
                        log.error(String.format("update file failed, move file failed, filename : [%s], filetype : [%s], " +
                                "taskTag : [%s]", fileName, fileType.getDir(), taskTag), e);
                    }
                }
            } else {
                log.error("update file failed, oss file not exist. oss path : [{}], taskTag : [{}]", ossFilePath, taskTag);
            }
        } catch (Exception e) {
            log.error(String.format("update file failed, oss operator failed, oss path : [%s], taskTag : [%s]",
                    ossFilePath, taskTag), e);
            return;
        } finally {
            //处理结束删除临时文件
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }


    private static boolean isChange(String sourceFile, String targetFile) {
        String sourceFileMd5 = LinuxProcessUtils.md5(sourceFile)
                                                .split(" ")[0];
        String targetFileMd5 = LinuxProcessUtils.md5(targetFile)
                                                .split(" ")[0];
        return !sourceFileMd5.equals(targetFileMd5);
    }


    private static void create(OSSClientHolder ossClientHolder, String ossFilePath, String filename, FileType fileType, String taskTag) {
        try {
            if (ossClientHolder.getOssClient()
                               .doesObjectExist(ossClientHolder.getBucketName(), ossFilePath)) {
                StringBuilder targetFilePath = new StringBuilder();
                targetFilePath.append(Constants.LOCAL_COMMON_SHELL_BASE_PATH)
                              .append(File.separator)
                              .append(fileType.getDir())
                              .append(File.separator)
                              .append(filename);
                OssUtils.downloadFileToLocal(ossClientHolder, ossFilePath, targetFilePath.toString());
                log.info("create file success, oss key : [{}], taskTag : [{}]", ossFilePath, taskTag);
            } else {
                log.error("create file failed, oss file not exist, oss key : [{}], taskTag : [{}]", ossFilePath, taskTag);
            }
        } catch (Exception e) {
            log.error(String.format("create file failed, oss operator failed, oss key : [%s], taskTag : [%s]", ossFilePath, taskTag), e);
        }
    }


    private static void delete(FileType fileType, String fileName, String taskTag) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(Constants.LOCAL_COMMON_SHELL_BASE_PATH)
                     .append(File.separator)
                     .append(fileType.getDir())
                     .append(File.separator)
                     .append(fileName);
        File targetFile = new File(stringBuilder.toString());
        if (targetFile.exists()) {
            try {
                targetFile.delete();
                log.info("delete file success, file path : [{}], taskTag : [{}]", stringBuilder.toString(), taskTag);
            } catch (Exception e) {
                log.error(String.format("delete file failed, throw exception, file path : [%s], taskTag : [%s]", stringBuilder.toString(), taskTag), e);
            }
        } else {
            log.warn("delete file failed, target file not exist, file path : [{}], taskTag : [{}]", stringBuilder.toString(), taskTag);
        }
    }


    private static String getFilename(String ossKey) {
        if (ossKey.indexOf(File.separator) != -1) {
            return ossKey.substring(ossKey.lastIndexOf(File.separator));
        }

        return ossKey;
    }
}
