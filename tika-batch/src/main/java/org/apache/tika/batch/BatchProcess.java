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


/**
 * This is the main processor class for a single process.
 * <p/>
 * It requires a {@link FileResourceCrawler} and {@link FileResourceConsumer}s, and it can also
 * support a {@link SimpleLogStatusReporter} and an {@link CommandLineInterrupter}.
 * <p/>
 * This is designed to shutdown if there are too many stale processors (runaway parser)
 * or an OutOfMemoryError. Consider using {@link BatchProcessDriverCLI}
 * for more robust handling of these types of errors.
 */
public class BatchProcess {

    public enum BATCH_CONSTANTS {
        BATCH_PROCESS_EXCEEDED_MAX_ALIVE_TIME,
        BATCH_PROCESS_FATAL_MUST_RESTART
    }

    private enum CAUSE_FOR_TERMINATION {
        COMPLETED_NORMALLY,
        MAIN_LOOP_EXCEPTION_NO_RESTART,
        MAIN_LOOP_EXCEPTION,
        CRAWLER_TIMED_OUT,
        TOO_MANY_STALES,
        USER_INTERRUPTION,
        BATCH_PROCESS_ALIVE_TOO_LONG,
    }

    private static final Log logger = LogFactory.getLog(BatchProcess.class);

    // If a file hasn't been processed in this amount of time,
    // report it to the console. When the directory crawler has stopped, the thread will
    // be terminated and the file name will be logged
    private long staleThresholdMillis = 5 * 60 * 1000; // 5 minutes

    private long staleCheckPulseMillis = 2 * 60 * 1000; //2 minutes
    //if there was an early termination via the Interrupter
    //or because of an uncaught runtime throwable, pause
    //this long before shutting down to allow parsers to finish
    private long pauseOnEarlyTerminationMillis = 30*1000; //30 seconds

    //maximum time that this process should stay alive
    //to avoid potential memory leaks, not a bad idea to shutdown
    //every hour or so.
    private int maxAliveTimeSeconds = -1;

    private final FileResourceCrawler fileResourceCrawler;

    private final ConsumersManager consumersManager;

    private final IStatusReporter reporter;

    private final IInterrupter interrupter;

    private final ArrayBlockingQueue<FileStarted> stales;

    private boolean alreadyExecuted = false;

    private final int maxStaleConsumers;

    public BatchProcess(FileResourceCrawler fileResourceCrawler,
                        ConsumersManager consumersManager,
                        IStatusReporter reporter,
                        IInterrupter interrupter, int maxStaleConsumers) {
        this.fileResourceCrawler = fileResourceCrawler;
        this.consumersManager = consumersManager;
        this.reporter = reporter;
        this.interrupter = interrupter;
        //parameter check, maxStaleConsumers must be >= 0
        this.maxStaleConsumers = (maxStaleConsumers > -1) ? maxStaleConsumers : 0;
        stales = new ArrayBlockingQueue<FileStarted>(consumersManager.getConsumers().size());
    }

    public ParallelFileProcessingResult execute()
            throws InterruptedException {
        if (alreadyExecuted) {
            throw new IllegalStateException("Can only execute BatchRunner once.");
        }
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
        // fileResourceCrawler, statusReporter, the Interrupter, staleChecker
        int numNonConsumers = 4;

        ExecutorService ex = Executors.newFixedThreadPool(numConsumers
                + numNonConsumers);

        CompletionService<IFileProcessorFutureResult> completionService =
                new ExecutorCompletionService<IFileProcessorFutureResult>(
                        ex);
        StaleChecker staleChecker = new StaleChecker();
        completionService.submit(interrupter);
        completionService.submit(fileResourceCrawler);
        completionService.submit(reporter);
        completionService.submit(staleChecker);


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
                        added += ((FileResourceCrawlerFutureResult) result).getAdded();
                        considered += ((FileResourceCrawlerFutureResult) result).getConsidered();
                        if (fileResourceCrawler.wasTimedOut()) {
                            causeForTermination = CAUSE_FOR_TERMINATION.CRAWLER_TIMED_OUT;
                            break;
                        }
                    } else if (result instanceof InterrupterFutureResult) {
                        causeForTermination = CAUSE_FOR_TERMINATION.USER_INTERRUPTION;
                        break;
                    } else if (result instanceof StaleFutureResult) {
                        causeForTermination = CAUSE_FOR_TERMINATION.TOO_MANY_STALES;
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


        //Step 1: prevent uncalled threads from being started
        ex.shutdown();

        //Step 2: ask consumers to retire politely.
        //Under normal circumstances, they should all have completed by now.
        for (FileResourceConsumer consumer : consumersManager.getConsumers()) {
            consumer.pleaseRetire();
        }

        //if there are any active/non-stale consumers, await termination
        //this can happen if a user interrupts the process
        //of if the crawler stops early
        if (countActiveConsumers() > 0) {
            if (causeForTermination != null) {
                logger.trace("About to awaitTermination: " + pauseOnEarlyTerminationMillis);
                ex.awaitTermination(pauseOnEarlyTerminationMillis, TimeUnit.MILLISECONDS);
            } else {
                ex.awaitTermination(1, TimeUnit.SECONDS);
            }
        }

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
                            && fileStarted.getElapsedMillis() > staleThresholdMillis) {
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
        } else if (causeForTermination == CAUSE_FOR_TERMINATION.TOO_MANY_STALES) {
            if (areResourcesPotentiallyRemaining()) {
                restartMsg = "Staled out with resources remaining";
            }
        } else if (causeForTermination == CAUSE_FOR_TERMINATION.BATCH_PROCESS_ALIVE_TOO_LONG) {
            restartMsg = BATCH_CONSTANTS.BATCH_PROCESS_EXCEEDED_MAX_ALIVE_TIME.toString();
        } else if (causeForTermination == CAUSE_FOR_TERMINATION.CRAWLER_TIMED_OUT) {
            restartMsg = "Crawler timed out.";
        } else if (crawlerRemoved == 0) {
            restartMsg = "Crawler did not complete.";
        } else if (fileResourceCrawler.wasTimedOut()) {
            restartMsg = "Crawler was timed out.";
        } else if (fileResourceCrawler.isActive()) {
            restartMsg = "Crawler is still active.";
        } else if (! fileResourceCrawler.isQueueEmpty()) {
            restartMsg = "Resources still exist for processing";
        }

        int exitStatus = getExitStatus(causeForTermination, restartMsg);

        //need to re-check, report, remove stale consumers
        staleChecker.checkForStaleConsumers();

        for (FileStarted fs : stales) {
            logger.error("A parser was still working on >" + fs.getResourceId() +
                    "< for " + fs.getElapsedMillis() + " milliseconds after it started." +
                    " This exceeds the maxStaleMillis parameter");
        }
        //Now we try to shutdown the ConsumersManager
        //TODO: put this in a separate thread and time it out if necessary.
        //a ConsumersManager shutdown hang would cause entire BatchProcess to hang.
        logger.info("ConsumersManager is shutting down");
        consumersManager.shutdown();
        logger.info("ConsumersManager has shut down");

        double elapsed = ((double) new Date().getTime() - (double) start) / 1000.0;
        return new
            ParallelFileProcessingResult(considered, added, processed,
                elapsed, exitStatus, causeForTermination.toString());
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
            return -1;
        }

        if (restartMsg != null) {
            if (restartMsg.equals(BATCH_CONSTANTS.BATCH_PROCESS_EXCEEDED_MAX_ALIVE_TIME.toString())) {
                logger.info(restartMsg);
            } else {
                logger.fatal(restartMsg);
            }
            //must send to err to communicate with potential driver
            System.err.println(
                    BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString() +
                            " >> " + restartMsg);
            System.err.flush();
            return 1;
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
     * If there is an early termination via an interrupt or too many stale consumers
     * or because a consumer or other Runnable threw a Throwable, pause this long
     * before killing the consumers and other threads.
     *
     * @param pauseOnEarlyTerminationMillis how long to pause if there is an early termination
     */
    public void setPauseOnEarlyTerminationMillis(long pauseOnEarlyTerminationMillis) {
        this.pauseOnEarlyTerminationMillis = pauseOnEarlyTerminationMillis;
    }

    /**
     * The amount of time allowed before a consumer is determined to be stale.
     *
     * @param staleThresholdMillis threshold in milliseconds before declaring a process stale
     */
    public void setStaleThresholdMillis(long staleThresholdMillis) {
        this.staleThresholdMillis = staleThresholdMillis;
    }

    public void setStaleCheckPulseMillis(long staleCheckPulseMillis) {
        this.staleCheckPulseMillis = staleCheckPulseMillis;
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

    private class StaleChecker implements Callable<IFileProcessorFutureResult> {

        @Override
        public StaleFutureResult call() throws Exception {
            while (stales.size() <= maxStaleConsumers) {
                try {
                    Thread.sleep(staleCheckPulseMillis);
                } catch (InterruptedException e) {
                    logger.debug("interrupted ex in stalechecker");
                    break;
                    //just stop.
                }
                checkForStaleConsumers();
                if (countActiveConsumers() == 0) {
                    logger.error("No activeConsumers in StaleChecker");
                    break;
                }
            }
            logger.debug("Stale checker quitting: " + stales.size());
            return new StaleFutureResult(stales.size());
        }

        private void checkForStaleConsumers() {
            for (FileResourceConsumer consumer : consumersManager.getConsumers()) {
                FileStarted fs = consumer.checkForStaleMillis(staleThresholdMillis);
                if (fs != null) {
                    stales.add(fs);
                }
            }
        }
    }

    private class StaleFutureResult implements IFileProcessorFutureResult {
        private final int staleCount;

        private StaleFutureResult(final int staleCount) {
            this.staleCount = staleCount;
        }

        protected int getStaleCount() {
            return staleCount;
        }
    }
}
