package io.github.artiship.arlo.scheduler.worker.test;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stream.LogOutputStream;


public class TestPythonLog {

    public static void main(String[] args) {

        String runCommand = "cd /Users/shell;sh start-all.sh";
        ProcessExecutor processExecutor = new ProcessExecutor();
        try {
            StartedProcess startedProcess = processExecutor.command("/bin/bash", "-c", runCommand)
                                                           .redirectError(new LogOutputStream() {
                                                               @Override
                                                               protected void processLine(String errorMsg) {
                                                                   System.out.println("Error : " + errorMsg);
                                                               }
                                                           })
                                                           .redirectOutput(new LogOutputStream() {
                                                               @Override
                                                               protected void processLine(String normalMsg) {
                                                                   System.out.println("Normal : " + normalMsg);
                                                               }
                                                           })
                                                           .destroyOnExit()
                                                           .start();

            int exitCode = startedProcess.getFuture()
                                         .get()
                                         .getExitValue();
            System.out.println("exit code : " + exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
