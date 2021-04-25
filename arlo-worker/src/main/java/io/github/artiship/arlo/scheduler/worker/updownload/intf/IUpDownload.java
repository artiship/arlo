package io.github.artiship.arlo.scheduler.worker.updownload.intf;


public interface IUpDownload {


    void download(String sourcePath, String targetPath) throws Exception;


    void uploadLog(String logContext);


    void close();


    void preprocess();

}
