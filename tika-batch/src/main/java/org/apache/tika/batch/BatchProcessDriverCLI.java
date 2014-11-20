package org.apache.tika.batch;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Locale;

import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.tika.io.IOUtils;
import org.apache.tika.util.BatchLocalization;

public class BatchProcessDriverCLI {

    public static final int PROCESS_RESTART_EXIT_CODE = -1;
    public static final int PROCESS_NO_RESTART_EXIT_CODE = 1;
    /**
     * This relies on an exit value of -1 (do not restart),
     * 0 ended correctly, 1 ended with exception
     * and should restart.
     */

    private static Logger logger = Logger.getLogger(BatchProcessDriverCLI.class);

    //TODO: need to set this!!!
    private int maxProcessRestarts = -1;
    private long pulseMillis = 1000;

    private boolean mustRestartProcess = false;
    private boolean userInterrupted = false;
    private Process process = null;

    private StreamGobbler errorGobbler = null;
    private StreamGobbler outGobbler = null;
    private StreamWriter stdinWriter = null;

    private Thread errorGobblerThread = null;
    private Thread outGobblerThread = null;
    private Thread stdinWriterThread = null;

    private final String[] commandLine;
    private int numRestarts = 0;

    private Options options = null;

    public BatchProcessDriverCLI(String[] commandLine){
        this.commandLine = commandLine;
    }

    public void execute() throws Exception {
        start();
        while (!userInterrupted) {
            int exit = Integer.MIN_VALUE;
            boolean hasExited = false;
            try {
                exit = process.exitValue();
                if (exit == Integer.MIN_VALUE) {
                    throw new IllegalArgumentException("Client process must never "+
                            "exit with value of Integer.MIN_VALUE!");
                }
                hasExited = true;
                stop();
            } catch (IllegalThreadStateException e) {
                //hasn't exited
            }
            //Even if the process has exited,
            //wait just a little bit to make sure that
            //mustRestart hasn't been set to true
            try {
                Thread.sleep(pulseMillis);
            } catch (InterruptedException e) {
                //swallow
            }
            if (hasExited && exit == 0 && ! mustRestartProcess) {
                break;
            }
            //no restart
            if (hasExited && (exit == BatchProcessDriverCLI.PROCESS_NO_RESTART_EXIT_CODE)) {
                break;
            }

            if ((hasExited && exit == BatchProcessDriverCLI.PROCESS_RESTART_EXIT_CODE)
                    || mustRestartProcess) {
                restart();
            }
        }
        shutdownNow();
    }

    private void shutdownNow() {
        if (process != null) {
            for (int i = 0; i < 10; i++) {
                try {
                    int exit = process.exitValue();
                    stop();
                    return;
                } catch (IllegalThreadStateException e) {
                    //hasn't exited
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    //swallow
                }
            }
            logger.error("Process didn't stop after 10 seconds after shutdown. " +
                    "I am forcefully killing it.");
        }
        stop();

    }

    public int getNumRestarts() {
        return numRestarts;
    }

    public boolean getUserInterrupted() {
        return userInterrupted;
    }

    private boolean restart() throws Exception {
        if (maxProcessRestarts > -1 && numRestarts >= maxProcessRestarts) {
            logger.warn("Exceeded the maximum number of process restarts. Driver is shutting down now.");
            stop();
            return false;
        }
        logger.warn("Must restart process ("+numRestarts+" restarts so far).");
        stop();
        start();
        numRestarts++;
        return true;
    }

    private void stop() {
        if (process != null) {
            process.destroy();
        }
        mustRestartProcess = false;
        errorGobbler.stopGobblingAndDie();
        outGobbler.stopGobblingAndDie();
        stdinWriter.stopGobblingAndDie();
        errorGobblerThread.interrupt();
        outGobblerThread.interrupt();
        stdinWriterThread.interrupt();
    }

    private void start() throws Exception {
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.directory(new File("."));
        process = builder.start();
        errorGobbler = new StreamGobbler(process.getErrorStream());
        errorGobblerThread = new Thread(errorGobbler);
        errorGobblerThread.start();

        outGobbler = new StreamGobbler(process.getInputStream());
        outGobblerThread = new Thread(outGobbler);
        outGobblerThread.start();

        stdinWriter = new StreamWriter(System.in, process.getOutputStream());
        stdinWriterThread = new Thread(stdinWriter);
        //this is a workaround to deal with the blocking readLine in StreamWriter
        //TODO: There has _got_ to be a better way.
        stdinWriterThread.setDaemon(true);
        stdinWriterThread.start();

    }


    /**
     * Class that transfers anything sent to stdin to the process driver
     * to the stdin of the BatchProcess.  This allows for a graceful interrupt.
     */
    private class StreamWriter implements Runnable {
        private InputStream is;
        private BufferedReader reader;
        private Writer writer;
        private boolean running = true;

        private StreamWriter(InputStream is, OutputStream os) {
            this.is = is;
            try {
                this.reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(is),
                        BatchLocalization.getEncoding()));
                this.writer = new OutputStreamWriter(os, BatchLocalization.getEncoding());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Unsupported encoding:" +
                        BatchLocalization.getEncoding());
            }
        }

        @Override
        public void run() {
            String line = null;
            try {
                if ((line = reader.readLine()) != null && this.running) {
                    writer.write(String.format(Locale.ENGLISH, "%s%n", line));
                    writer.flush();
                    userInterrupted = true;
                }
            } catch (IOException e) {
                //swallow ioe
            }
        }

        private void stopGobblingAndDie() {
            try {
                is.close();
            } catch (IOException e) {
                //swallow
            }
            running = false;
        }
    }

    private class StreamGobbler implements Runnable {
        //plagiarized from oap.oodt's StreamGobbler
        private final BufferedReader reader;
        private boolean running = true;

        private StreamGobbler(InputStream is){

            try {
                this.reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(is),
                        BatchLocalization.getEncoding()));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UnsupportedEncodingException: "+
                        BatchLocalization.getEncoding());
            }
        }

        @Override
        public void run() {
            String line = null;
            try {
                while ((line = reader.readLine()) != null && this.running) {
                    if (line.startsWith(BatchProcess.BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString())) {
                        mustRestartProcess = true;
                    }
                    logger.info("BatchProcess: "+line);
                }
            } catch (IOException e) {
                //swallow ioe
            }
        }

        private void stopGobblingAndDie() {
            running = false;
            IOUtils.closeQuietly(reader);
        }
    }


    public static void main(String[] args) throws Exception {

        BatchProcessDriverCLI runner = new BatchProcessDriverCLI(args);
        runner.execute();
        System.out.println("FSBatchProcessDriver has gracefully completed");
        System.exit(0);
    }
}
