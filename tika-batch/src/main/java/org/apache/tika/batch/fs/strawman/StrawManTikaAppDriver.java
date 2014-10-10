package org.apache.tika.batch.fs.strawman;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.tika.io.IOUtils;

/**
 * Simple single-threaded class that calls tika-app against every file in a directory.
 *
 * This is exceedingly robust.  One file per process.
 *
 * However, you can use this to compare performance against tika-batch fs code.
 *
 *
 */
public class StrawManTikaAppDriver implements Callable<Integer> {

    private static AtomicInteger threadCount = new AtomicInteger(0);
    private final int totalThreads;
    private final int threadNum;
    private int rootLen = -1;
    private File srcDir = null;
    private File targDir = null;
    private String[] args = null;
    private Logger logger = Logger.getLogger(StrawManTikaAppDriver.class);


    public StrawManTikaAppDriver(File srcDir, File targDir, int totalThreads, String[] args) {
        rootLen = srcDir.getAbsolutePath().length()+1;
        this.srcDir = srcDir;
        this.targDir = targDir;
        this.args = args;
        threadNum = threadCount.getAndIncrement();
        this.totalThreads = totalThreads;
    }


    private int processDirectory(File srcDir) {
        int processed = 0;
        if (srcDir == null || srcDir.listFiles() == null) {
            return processed;
        }
        for (File f : srcDir.listFiles()) {
            List<File> childDirs = new ArrayList<File>();
            if (f.isDirectory()) {
                childDirs.add(f);
            } else {
                processed += processFile(f);
            }
            for (File dir : childDirs) {
                processed += processDirectory(dir);

            }
        }
        return processed;
    }

    private int processFile(File f) {
        if (totalThreads > 1) {
            int hashCode = f.getAbsolutePath().hashCode();
            if (Math.abs(hashCode % totalThreads) != threadNum) {
                return 0;
            }
        }
        File targFile = new File(targDir, f.getAbsolutePath().substring(rootLen)+".txt");
        targFile.getAbsoluteFile().getParentFile().mkdirs();
        if (! targFile.getParentFile().exists()) {
            logger.fatal("parent directory for "+ targFile + " was not made!");
            throw new RuntimeException("couldn't make parent file for " + targFile);
        }
        List<String> commandLine = new ArrayList<String>();
        for (int i = 0; i < args.length; i++) {
            commandLine.add(args[i]);
        }
        commandLine.add("-t");
        commandLine.add("\""+f.getAbsolutePath()+"\"");
        ProcessBuilder builder = new ProcessBuilder(commandLine.toArray(new String[commandLine.size()]));
        logger.info("about to process: "+f.getAbsolutePath());
        Process proc = null;
        RedirectGobbler gobbler = null;
        Thread gobblerThread = null;
        try {
            OutputStream os = new FileOutputStream(targFile);
            proc = builder.start();
            gobbler = new RedirectGobbler(proc.getInputStream(), os);
            gobblerThread = new Thread(gobbler);
            gobblerThread.start();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return 0;
        }

        boolean finished = false;
        long totalTime = 180000;//3 minutes
        long pulse = 100;
        for (int i = 0; i < totalTime; i += pulse) {
            try {
                Thread.currentThread().sleep(pulse);
            } catch (InterruptedException e) {
                //swallow
            }
            try {
                int exit = proc.exitValue();
                finished = true;
                break;
            } catch (IllegalThreadStateException e) {
                //swallow
            }
        }
        if (!finished) {
            logger.warn("Had to kill process working on: " + f.getAbsolutePath());
            proc.destroy();
        }
        gobbler.close();
        gobblerThread.interrupt();
        return 1;
    }


    @Override
    public Integer call() throws Exception {
        long start = new Date().getTime();

        int processed = processDirectory(srcDir);
        double elapsedSecs = ((double)new Date().getTime()-(double)start)/(double)1000;
        logger.info("Finished processing " + processed + " files in " + elapsedSecs + " seconds.");
        return new Integer(processed);
    }

    private class RedirectGobbler implements Runnable {
        private OutputStream os = null;
        private InputStream is = null;

        private RedirectGobbler(InputStream is, OutputStream os) {
            this.is = is;
            this.os = os;
        }
        private void gobble(InputStream is) {
        }
        private void close() {
            if (os != null) {
                try {
                    os.flush();
                } catch (IOException e) {
                    logger.error("can't flush");
                }
                try {
                    os.close();
                    is.close();
                } catch (IOException e) {
                    logger.error("can't close");
                }
            }
        }

        @Override
        public void run() {
            try {
                IOUtils.copy(is, os);
            } catch (IOException e) {
                logger.error("IOException while gobbling");
            }
        }
    }

    public static String usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Example usage:\n");
        sb.append("java -cp <CP> org.apache.batch.fs.strawman.StrawManTikaAppDriver ");
        sb.append("<srcDir> <targDir> <numThreads> ");
        sb.append("java -jar tika-app-X.Xjar <...commandline arguments for tika-app>\n\n");
        return sb.toString();
    }

    public static void main(String[] args) {
        long start = new Date().getTime();
        if (args.length < 6) {
            System.err.println(StrawManTikaAppDriver.usage());
        }
        File srcDir = new File(args[0]);
        File targDir = new File(args[1]);
        int totalThreads = Integer.parseInt(args[2]);

        List<String> commandLine = new ArrayList<String>();
        for (int i = 3; i < args.length; i++) {
            commandLine.add(args[i]);
        }
        totalThreads = (totalThreads < 1) ? 1 : totalThreads;
        ExecutorService ex = Executors.newFixedThreadPool(totalThreads);
        ExecutorCompletionService<Integer> completionService =
                new ExecutorCompletionService<Integer>(ex);

        for (int i = 0; i < totalThreads; i++) {
            StrawManTikaAppDriver driver =
                    new StrawManTikaAppDriver(srcDir, targDir, totalThreads, commandLine.toArray(new String[commandLine.size()]));
            completionService.submit(driver);
        }

        int totalFilesProcessed = 0;
        for (int i = 0; i < totalThreads; i++) {
            try {
                Future<Integer> future = completionService.take();
                if (future != null) {
                    totalFilesProcessed += future.get();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        double elapsedSeconds = (double)(new Date().getTime()-start)/(double)1000;
        System.out.println("Processed "+totalFilesProcessed + " in " + elapsedSeconds + " seconds");
    }
}
