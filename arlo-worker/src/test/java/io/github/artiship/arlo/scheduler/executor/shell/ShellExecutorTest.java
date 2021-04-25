//package io.github.artiship.arlo.scheduler.executor.shell;
//
//import org.junit.Test;
//
//import java.io.BufferedReader;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.util.concurrent.Executors;
//import java.util.function.Consumer;
//
//import static org.junit.Assert.assertEquals;
//
//public class ShellExecutorTest {
//
//    @Test
//    public void run_shell() throws Exception {
//        String homeDirectory = System.getProperty("user.home");
//        Process process = Runtime.getRuntime()
//                                 .exec(String.format("sh -c ls %s", homeDirectory));
//
//        StreamGobbler streamGobbler =
//                new StreamGobbler(process.getInputStream(), System.out::println);
//        Executors.newSingleThreadExecutor().submit(streamGobbler);
//        int exitCode = process.waitFor();
//
//        assertEquals(exitCode, 0);
//    }
//
//    private static class StreamGobbler implements Runnable {
//        private InputStream inputStream;
//        private Consumer<String> consumer;
//
//        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
//            this.inputStream = inputStream;
//            this.consumer = consumer;
//        }
//
//        @Override
//        public void run() {
//            new BufferedReader(new InputStreamReader(inputStream)).lines()
//                                                                  .forEach(consumer);
//        }
//    }
//}