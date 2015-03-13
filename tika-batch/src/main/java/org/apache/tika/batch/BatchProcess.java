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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.io.IOUtils;


/**
 * This is the main processor class for a single process.
 * <p/>
 * It requires a {@link FileResourceCrawler} and {@link FileResourceConsumer}s, and it can also
 * support a {@link SimpleLogStatusReporter} and an {@link CommandLineInterrupter}.
 * <p/>
 * This is designed to shutdown if a parser has timed out or if there is
 * an OutOfMemoryError. Consider using {@link BatchProcessDriverCLI}
 * as a daemon/watchdog that monitors and can restart this batch process;
 * <p>
 * Note that this classs redirects stderr to stdout so that it can
 * communicate without interference with the parent process on stderr.
 */
public class BatchProcess implements Callable<ParallelFileProcessingResult> {

    public enum BATCH_CONSTANTS {
        BATCH_PROCESS_EXCEEDED_MAX_ALIVE_TIME,
        BATCH_PROCESS_FATAL_MUST_RESTART
    }

    private enum CAUSE_FOR_TERMINATION {
        COMPLETED_NORMALLY,
        MAIN_LOOP_EXCEPTION_NO_RESTART,
        MAIN_LOOP_EXCEPTION,
        CRAWLER_TIMED_OUT,
        TIMED_OUT_CONSUMER,
        USER_INTERRUPTION,
        BATCH_PROCESS_ALIVE_TOO_LONG,
    }

    private static final Log logger;
    static {
        logger = LogFactory.getLog(BatchProcess.class);
    }

    private PrintStream outputStreamWriter;

    // If a file hasn't been processed in this amount of time,
    // report it to the console. When the directory crawler has stopped, the thread will
    // be terminated and the file name will be logged
    private long timeoutThresholdMillis = 5 * 60 * 1000; // 5 minutes

    private long timeoutCheckPulseMillis = 2 * 60 * 1000; //2 minutes
    //if there was an early termination via the Interrupter
    //or because of an uncaught runtime throwable, pause
    //this long before shutting down to allow parsers to finish
    private long pauseOnEarlyTerminationMillis = 30*1000; //30 seconds

    private long consumersManagerShutdownMillis = 60*1000;

    //maximum time that this process should stay alive
    //to avoid potential memory leaks, not a bad idea to shutdown
    //every hour or so.
    private int maxAliveTimeSeconds = -1;

    private final FileResourceCrawler fileResourceCrawler;

    private final ConsumersManager consumersManager;

    private final IStatusReporter reporter;

    private final IInterrupter interrupter;

    private final ArrayBlockingQueue<FileStarted> timedOuts;

    private boolean alreadyExecuted = false;

    public BatchProcess(FileResourceCrawler fileResourceCrawler,
                        ConsumersManager consumersManager,
                        IStatusReporter reporter,
                        IInterrupter interrupter) {
        this.fileResourceCrawler = fileResourceCrawler;
        this.consumersManager = consumersManager;
        this.reporter = reporter;
        this.interrupter = interrupter;
        timedOuts = new ArrayBlockingQueue<FileStarted>(consumersManager.getConsumers().size());
    }

    /**
     * Runs main execution loop.
     * <p>
     * Redirects stdout to stderr to keep clean communications
     * over stdout with parent process
     * @return result of the processing
     * @throws InterruptedException
     */
    public ParallelFileProcessingResult call()
            throws InterruptedException {
        if (alreadyExecuted) {
            throw new IllegalStateException("Can only execute BatchRunner once.");
        }
        //redirect streams
        try {
            outputStreamWriter = new PrintStream(System.err, true, IOUtils.UTF_8.toString());
        } catch (IOException e) {
            throw new RuntimeException("Can't redirect streams");
        }
        System.setErr(System.out);

        Runtime.getRuntime().addShutdownHook(getConsumersManagerShutdownThread());

        long start = new Date().getTime();
        logger.info("BatchProcess starting up");
        //TODO: hope that this doesn't hang. :)
        consumersManager.init();
        alreadyExecuted = true;
        long started = new Date().getTime();
        int considered = 0;
        int added = 0;
        int processed = 0;
        int numConsumers = consumersManager.getConsumers().size();
        // fileResourceCrawler, statusReporter, the Interrupter, timeoutChecker
        int numNonConsumers = 4;

        ExecutorService ex = Executors.newFixedThreadPool(numConsumers
                + numNonConsumers);

        CompletionService<IFileProcessorFutureResult> completionService =
                new ExecutorCompletionService<IFileProcessorFutureResult>(
                        ex);
        TimeoutChecker timeoutChecker = new TimeoutChecker();
        completionService.submit(interrupter);
        completionService.submit(fileResourceCrawler);
        completionService.submit(reporter);
        completionService.submit(timeoutChecker);


        for (FileResourceConsumer consumer : consumersManager.getConsumers()) {
            completionService.submit(consumer);
        }

        int removed = 0;
        int consumersRemoved = 0;
        //room to grow...currently only one crawler
        int crawlerRemoved = 0;

        CAUSE_FOR_TERMINATION causeForTermination = null;
        //main processing loop
        while (true) {
            try {
                Future<IFileProcessorFutureResult> futureResult =
                        completionService.poll(1, TimeUnit.SECONDS);

                if (futureResult != null) {
                    removed++;
                    IFileProcessorFutureResult result = futureResult.get();
                    if (result instanceof FileConsumerFutureResult) {
                        consumersRemoved++;
                        processed += ((FileConsumerFutureResult) result).getFilesProcessed();
                    } else if (result instanceof FileResourceCrawlerFutureResult) {
                        crawlerRemoved++;
                        if (fileResourceCrawler.wasTimedOut()) {
                            causeForTermination = CAUSE_FOR_TERMINATION.CRAWLER_TIMED_OUT;
                            break;
                        }
                    } else if (result instanceof InterrupterFutureResult) {
                        causeForTermination = CAUSE_FOR_TERMINATION.USER_INTERRUPTION;
                        break;
                    } else if (result instanceof TimeoutFutureResult) {
                        causeForTermination = CAUSE_FOR_TERMINATION.TIMED_OUT_CONSUMER;
                        break;
                    } //only thing left should be StatusReporterResult
                }

                if (consumersRemoved >= numConsumers) {
                    causeForTermination = CAUSE_FOR_TERMINATION.COMPLETED_NORMALLY;
                    break;
                }
                if (aliveTooLong(started)) {
                    causeForTermination = CAUSE_FOR_TERMINATION.BATCH_PROCESS_ALIVE_TOO_LONG;
                    break;
                }
            } catch (Throwable e) {
                if (isNonRestart(e)) {
                    causeForTermination = CAUSE_FOR_TERMINATION.MAIN_LOOP_EXCEPTION_NO_RESTART;
                } else {
                    causeForTermination = CAUSE_FOR_TERMINATION.MAIN_LOOP_EXCEPTION;
                }
                logger.fatal("Main loop execution exception: " + e.getMessage());
                break;
            }
        }
        reporter.setIsShuttingDown(true);
        added = fileResourceCrawler.getAdded();
        considered = fileResourceCrawler.getConsidered();

        //TODO: figure out safe way to shutdown resource crawler
        //if it isn't.  Does it need to add poison at this point?
        //fileResourceCrawler.pleaseShutdown();

        //Step 1: prevent uncalled threads from being started
        ex.shutdown();

        //Step 2: ask consumers to shutdown politely.
        //Under normal circumstances, they should all have completed by now.
        for (FileResourceConsumer consumer : consumersManager.getConsumers()) {
            consumer.pleaseShutdown();
        }
        fileResourceCrawler.shutDownNoPoison();
        //if there are any active/asked to shutdown consumers, await termination
        //this can happen if a user interrupts the process
        //of if the crawler stops early, or ...
        politelyAwaitTermination(causeForTermination);

        //Step 3: Gloves come off.  We've tried to ask kindly before.
        //Now it is time shut down. This will corrupt
        //nio channels via thread interrupts!  Hopefully, everything
        //has shut down by now.
        logger.trace("About to shutdownNow()");
        List<Runnable> neverCalled = ex.shutdownNow();
        logger.trace("TERMINATED " + ex.isTerminated() + " : "
                + consumersRemoved + " : " + crawlerRemoved);

        int end = numConsumers + numNonConsumers - removed - neverCalled.size();

        for (int t = 0; t < end; t++) {
            Future<IFileProcessorFutureResult> future = null;
            future = completionService.poll(10, TimeUnit.MILLISECONDS);

            logger.trace("In while future==null loop in final shutdown loop");
            if (future == null) {
                break;
            }
            try {
                IFileProcessorFutureResult result = future.get();
                if (result instanceof FileConsumerFutureResult) {
                    FileConsumerFutureResult consumerResult = (FileConsumerFutureResult) result;
                    processed += consumerResult.getFilesProcessed();
                    FileStarted fileStarted = consumerResult.getFileStarted();
                    if (fileStarted != null
                            && fileStarted.getElapsedMillis() > timeoutThresholdMillis) {
                        logger.warn(fileStarted.getResourceId()
                                + "\t caused a file processor to hang or crash. You may need to remove "
                                + "this file from your input set and rerun.");
                    }
                } else if (result instanceof FileResourceCrawlerFutureResult) {
                    FileResourceCrawlerFutureResult crawlerResult = (FileResourceCrawlerFutureResult) result;
                    considered += crawlerResult.getConsidered();
                    added += crawlerResult.getAdded();
                } //else ...we don't care about anything else stopping at this point
            } catch (ExecutionException e) {
                logger.error("Execution exception trying to shutdown after shutdownNow:" + e.getMessage());
            } catch (InterruptedException e) {
                logger.error("Interrupted exception trying to shutdown after shutdownNow:" + e.getMessage());
            }
        }
        //do we need to restart?
        String restartMsg = null;
        if (causeForTermination == CAUSE_FOR_TERMINATION.USER_INTERRUPTION
                || causeForTermination == CAUSE_FOR_TERMINATION.MAIN_LOOP_EXCEPTION_NO_RESTART) {
            //do not restart!!!
        } else if (causeForTermination == CAUSE_FOR_TERMINATION.MAIN_LOOP_EXCEPTION) {
            restartMsg = "Uncaught consumer throwable";
        } else if (causeForTermination == CAUSE_FOR_TERMINATION.TIMED_OUT_CONSUMER) {
            if (areResourcesPotentiallyRemaining()) {
                restartMsg = "Consumer timed out with resources remaining";
            }
        } else if (causeForTermination == CAUSE_FOR_TERMINATION.BATCH_PROCESS_ALIVE_TOO_LONG) {
            restartMsg = BATCH_CONSTANTS.BATCH_PROCESS_EXCEEDED_MAX_ALIVE_TIME.toString();
        } else if (causeForTermination == CAUSE_FOR_TERMINATION.CRAWLER_TIMED_OUT) {
            restartMsg = "Crawler timed out.";
        } else if (fileResourceCrawler.wasTimedOut()) {
            restartMsg = "Crawler was timed out.";
        } else if (fileResourceCrawler.isActive()) {
            restartMsg = "Crawler is still active.";
        } else if (! fileResourceCrawler.isQueueEmpty()) {
            restartMsg = "Resources still exist for processing";
        }

        int exitStatus = getExitStatus(causeForTermination, restartMsg);

        //need to re-check, report, mark timed out consumers
        timeoutChecker.checkForTimedOutConsumers();

        for (FileStarted fs : timedOuts) {
            logger.fatal("A parser was still working on >" + fs.getResourceId() +
                    "< for " + fs.getElapsedMillis() + " milliseconds after it started." +
                    " This exceeds the maxTimeoutMillis parameter");
        }


        double elapsed = ((double) new Date().getTime() - (double) start) / 1000.0;
        return new
            ParallelFileProcessingResult(considered, added, processed,
                elapsed, exitStatus, causeForTermination.toString());
    }

    /**
     * This is used instead of awaitTermination(), because that interrupts
     * the thread and then waits for its termination.  This politely waits.
     *
     * @param causeForTermination reason for termination.
     */
    private void politelyAwaitTermination(CAUSE_FOR_TERMINATION causeForTermination) {
        if (causeForTermination == CAUSE_FOR_TERMINATION.COMPLETED_NORMALLY) {
            return;
        }
        long start = new Date().getTime();
        while (countActiveConsumers() > 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                //swallow
            }
            long elapsed = new Date().getTime()-start;
            if (pauseOnEarlyTerminationMillis > -1 &&
                    elapsed > pauseOnEarlyTerminationMillis) {
                logger.warn("I waited after an early termination for "+
                elapsed + ", but there was at least one active consumer");
                return;
            }
        }
    }

    private boolean isNonRestart(Throwable e) {
        if (e instanceof BatchNoRestartError) {
            return true;
        }
        Throwable cause = e.getCause();
        return cause != null && isNonRestart(cause);
    }

    private int getExitStatus(CAUSE_FOR_TERMINATION causeForTermination, String restartMsg) {
        if (causeForTermination == CAUSE_FOR_TERMINATION.MAIN_LOOP_EXCEPTION_NO_RESTART) {
            logger.info(CAUSE_FOR_TERMINATION.MAIN_LOOP_EXCEPTION_NO_RESTART);
            return BatchProcessDriverCLI.PROCESS_NO_RESTART_EXIT_CODE;
        }

        if (restartMsg != null) {
            if (restartMsg.equals(BATCH_CONSTANTS.BATCH_PROCESS_EXCEEDED_MAX_ALIVE_TIME.toString())) {
                logger.warn(restartMsg);
            } else {
                logger.fatal(restartMsg);
            }

            //send over stdout wrapped in outputStreamWriter
            outputStreamWriter.println(
                    BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString() +
                            " >> " + restartMsg);
            outputStreamWriter.flush();
            return BatchProcessDriverCLI.PROCESS_RESTART_EXIT_CODE;
        }
        return 0;
    }

    //could new FileResources be consumed from the Queue?
    //Because of race conditions, this can return a true
    //when the real answer is false.
    //This should never return false, though, if the answer is true!
    private boolean areResourcesPotentiallyRemaining() {
        if (fileResourceCrawler.isActive()) {
            return true;
        }
        return !fileResourceCrawler.isQueueEmpty();
    }

    private boolean aliveTooLong(long started) {
        if (maxAliveTimeSeconds < 0) {
            return false;
        }
        double elapsedSeconds = (double) (new Date().getTime() - started) / (double) 1000;
        return elapsedSeconds > (double) maxAliveTimeSeconds;
    }

    //snapshot of non-retired consumers; actual number may be smaller by the time
    //this returns a value!
    private int countActiveConsumers() {
        int active = 0;
        for (FileResourceConsumer consumer : consumersManager.getConsumers()) {
            if (consumer.isStillActive()) {
                active++;
            }
        }
        return active;
    }

    /**
     * If there is an early termination via an interrupt or too many timed out consumers
     * or because a consumer or other Runnable threw a Throwable, pause this long
     * before killing the consumers and other threads.
     *
     * Typically makes sense for this to be the same or slightly larger than
     * timeoutThresholdMillis
     *
     * @param pauseOnEarlyTerminationMillis how long to pause if there is an early termination
     */
    public void setPauseOnEarlyTerminationMillis(long pauseOnEarlyTerminationMillis) {
        this.pauseOnEarlyTerminationMillis = pauseOnEarlyTerminationMillis;
    }

    /**
     * The amount of time allowed before a consumer should be timed out.
     *
     * @param timeoutThresholdMillis threshold in milliseconds before declaring a consumer timed out
     */
    public void setTimeoutThresholdMillis(long timeoutThresholdMillis) {
        this.timeoutThresholdMillis = timeoutThresholdMillis;
    }

    public void setTimeoutCheckPulseMillis(long timeoutCheckPulseMillis) {
        this.timeoutCheckPulseMillis = timeoutCheckPulseMillis;
    }

    /**
     * The maximum amount of time that this process can be alive.  To avoid
     * memory leaks, it is sometimes beneficial to shutdown (and restart) the
     * process periodically.
     * <p/>
     * If the value is < 0, the process will run until completion, interruption or exception.
     *
     * @param maxAliveTimeSeconds maximum amount of time in seconds to remain alive
     */
    public void setMaxAliveTimeSeconds(int maxAliveTimeSeconds) {
        this.maxAliveTimeSeconds = maxAliveTimeSeconds;
    }

    public void setConsumersManagerShutdownMillis(long millis) {
        consumersManagerShutdownMillis = millis;
    }

    private Thread getConsumersManagerShutdownThread() {
        Thread thread =  new Thread() {
            public void run() {
                //start a new thread that can be timed
                Thread timed = new Thread() {
                    public void run() {
                        logger.trace("starting to shutdown consumers manager");
                        consumersManager.shutdown();
                        logger.trace("finished shutting down consumers manager");
                    };
                };
                timed.start();
                try {
                    timed.join(consumersManagerShutdownMillis);
                } catch (InterruptedException e) {
                    logger.warn("interruption exception during consumers manager shutdown");
                }
                if (timed.isAlive()) {
                    logger.error("ConsumersManager was still alive during shutdown!");
                }
            }
        };
        return thread;
    }

    private class TimeoutChecker implements Callable<IFileProcessorFutureResult> {

        @Override
        public TimeoutFutureResult call() throws Exception {
            while (timedOuts.size() == 0) {
                try {
                    Thread.sleep(timeoutCheckPulseMillis);
                } catch (InterruptedException e) {
                    logger.debug("Thread interrupted exception in TimeoutChecker");
                    break;
                    //just stop.
                }
                checkForTimedOutConsumers();
                if (countActiveConsumers() == 0) {
                    logger.error("No activeConsumers in TimeoutChecker");
                    break;
                }
            }
            logger.debug("TimeoutChecker quitting: " + timedOuts.size());
            return new TimeoutFutureResult(timedOuts.size());
        }

        private void checkForTimedOutConsumers() {
            for (FileResourceConsumer consumer : consumersManager.getConsumers()) {
                FileStarted fs = consumer.checkForTimedOutMillis(timeoutThresholdMillis);
                if (fs != null) {
                    timedOuts.add(fs);
                }
            }
        }
    }

    private class TimeoutFutureResult implements IFileProcessorFutureResult {
        private final int timedOutCount;

        private TimeoutFutureResult(final int timedOutCount) {
            this.timedOutCount = timedOutCount;
        }

        protected int getTimedOutCount() {
            return timedOutCount;
        }
    }
}
