package io.github.artiship.arlo.scheduler.worker.updownload.impl;

import io.github.artiship.arlo.scheduler.worker.updownload.intf.IUpDownload;


public abstract class AbstractUpDownload implements IUpDownload {

    @Override
    public void download(String sourcePath, String targetPath) throws Exception {

    }

    @Override
    public void uploadLog(String logContext) {

    }

    @Override
    public void close() {

    }

    @Override
    public void preprocess() {

    }
}
