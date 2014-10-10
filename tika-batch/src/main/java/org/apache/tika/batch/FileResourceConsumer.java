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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;


/**
 * This is a base class for file consumers. The
 * goal of this class is to abstract out the multithreading
 * and recordkeeping components.
 * <p/>
 */
public abstract class FileResourceConsumer implements Callable<IFileProcessorFutureResult> {




    private static enum STATE {
        NOT_YET_STARTED,
        ACTIVELY_CONSUMING,
        SWALLOWED_POISON,
        THREAD_INTERRUPTED,
        EXCEEDED_MAX_CONSEC_WAIT_MILLIS,
        RETIRED,
        GONE_STALE,
        CONSUMER_EXCEPTION,
        CONSUMER_ERROR,
        COMPLETED
    }
    private static AtomicInteger numConsumers = new AtomicInteger(-1);
    private static Logger logger = Logger.getLogger(FileResourceConsumer.class);

    private long maxConsecWaitInMillis = 10*60*1000;// 10 minutes

    private final ArrayBlockingQueue<FileResource> fileQueue;

    private final int consumerId;

    //used to lock checks on state to prevent
    private final Object lock = new Object();

    //this records the file that is currently
    //being processed.  It is null if no file is currently being processed.
    //no need for volatile because of lock for checkForStales
    private FileStarted currentFile = null;

    //total number of files consumed; volatile so that reporter
    //sees the latest
    private volatile int numResourcesConsumed = 0;

    //total number of exceptions that were handled by subclasses;
    //volatile so that reporter sees the latest
    private volatile int numHandledExceptions = 0;

    //after this has been set to ACTIVELY_CONSUMING,
    //this should only be set by setEndedState.
    private volatile STATE currentState = STATE.NOT_YET_STARTED;

    public FileResourceConsumer(ArrayBlockingQueue<FileResource> fileQueue) {
        this.fileQueue = fileQueue;
        consumerId = numConsumers.incrementAndGet();
    }

    public IFileProcessorFutureResult call() {
        currentState = STATE.ACTIVELY_CONSUMING;

        try {
            FileResource fileResource = getNextFileResource();
            while (fileResource != null) {
                logger.debug("file consumer is about to process: " + fileResource.getResourceId());
                boolean consumed = _processFileResource(fileResource);
                logger.debug("file consumer has finished processing: " + fileResource.getResourceId());

                if (consumed) {
                    numResourcesConsumed++;
                }
                fileResource = getNextFileResource();
            }
        } catch (InterruptedException e) {
            setEndedState(STATE.THREAD_INTERRUPTED);
        }

        setEndedState(STATE.COMPLETED);
        return new FileConsumerFutureResult(currentFile, numResourcesConsumed);
    }


    /**
     * Main piece of code that needs to be implemented.  Clients
     * are responsible for closing streams and handling the exceptions
     * that they'd like to handle.
     * <p/>
     * Unchecked throwables can be thrown past this, of course.  When an unchecked
     * throwable is thrown, this logs the error, and then rethrows the exception.
     * Clients/subclasses should make sure to catch and handle everything they can.
     * <p/>
     * The design goal is that the whole process should close up and shutdown soon after
     * an unchecked exception or error is thrown.
     * <p/>
     * Make sure to call {@link #incrementHandledExceptions()} appropriately in
     * your implementation of this method.
     * <p/>
     *
     * @param fileResource resource to process
     * @return whether or not a file was successfully processed
     */
    public abstract boolean processFileResource(FileResource fileResource);


    /**
     * Make sure to call this appropriately!
     */
    protected void incrementHandledExceptions() {
        numHandledExceptions++;
    }


    public boolean isStillActive() {
        return (! Thread.currentThread().isInterrupted() &&
                (currentState == STATE.NOT_YET_STARTED ||
                currentState == STATE.ACTIVELY_CONSUMING));
    }

    private boolean _processFileResource(FileResource fileResource) {
        currentFile = new FileStarted(fileResource.getResourceId());
        boolean consumed = false;
        try {
            consumed = processFileResource(fileResource);
        } catch (RuntimeException e) {
            setEndedState(STATE.CONSUMER_EXCEPTION);
            throw e;
        } catch (Error e) {
            setEndedState(STATE.CONSUMER_ERROR);
            throw e;
        }
        //if anything is thrown from processFileResource, then the fileStarted
        //will remain what it was right before the exception was thrown.
        currentFile = null;
        return consumed;
    }

    /**
     * This politely asks the consumer to shutdown.
     * Before processing another file, the consumer will check to see
     * if it has been asked to terminate.
     * <p>
     * This offers another method for politely requesting
     * that a FileResourceConsumer stop processing
     * besides passing it {@link org.apache.tika.batch.PoisonFileResource}.
     *
     */
    public void pleaseRetire() {
        setEndedState(STATE.RETIRED);
    }

    /**
     * Returns the name and start time of a file that is currently being processed.
     * If no file is currently being processed, this will return null.
     *
     * @return FileStarted or null
     */
    public FileStarted getCurrentFile() {
        return currentFile;
    }

    public int getNumResourcesConsumed() {
        return numResourcesConsumed;
    }

    public int getNumHandledExceptions() {
        return numHandledExceptions;
    }

    /**
     * Checks to see if the currentFile being processed (if there is one)
     * has gone stale (still being worked on after staleThresholdMillis).
     * <p>
     * If the consumer has gone stale, this will return the currentFile and
     * set the state to GONE_STALE.
     * <p>
     * If the consumer was already staled out earlier or
     * is not processing a file or has been working on a file
     * for less than #staleThresholdMills, then this will return null.
     * <p>
     * @param staleThresholdMillis threshold to determine whether the consumer has gone stale.
     * @return null or the file started that triggered the stale condition
     */
    public FileStarted checkForStaleMillis(long staleThresholdMillis) {
        //if it isn't actually running, don't bother obtaining lock
        if (currentState != STATE.ACTIVELY_CONSUMING) {
            return null;
        }
        synchronized(lock) {
            //check again once the lock has been obtained
            if (currentState != STATE.ACTIVELY_CONSUMING) {
                return null;
            }
            FileStarted tmp = currentFile;
            if (tmp == null) {
                return null;
            }
            if (tmp.getElapsedMillis() > staleThresholdMillis) {
                setEndedState(STATE.GONE_STALE);
                return tmp;
            }
        }
        return null;
    }

    private FileResource getNextFileResource() throws InterruptedException {
        FileResource fileResource = null;
        long start = new Date().getTime();
        while (fileResource == null) {
            //check to see if thread is interrupted before polling
            if (Thread.currentThread().isInterrupted()) {
                setEndedState(STATE.THREAD_INTERRUPTED);
                logger.debug("Consumer thread was interrupted.");
                break;
            }

            synchronized(lock) {
                //need to lock here to prevent race condition with other threads setting state
                if (currentState != STATE.ACTIVELY_CONSUMING) {
                    logger.debug("Consumer already closed because of: "+ currentState.toString());
                    break;
                }
            }
            fileResource = fileQueue.poll(1L, TimeUnit.SECONDS);
            if (fileResource != null) {
                if (fileResource instanceof PoisonFileResource) {
                    setEndedState(STATE.SWALLOWED_POISON);
                    fileResource = null;
                }
                break;
            }
            logger.debug(consumerId + " is waiting for file and the queue size is: " + fileQueue.size());

            long elapsed = new Date().getTime() - start;
            if (maxConsecWaitInMillis > 0 && elapsed > maxConsecWaitInMillis) {
                setEndedState(STATE.EXCEEDED_MAX_CONSEC_WAIT_MILLIS);
                break;
            }
        }
        return fileResource;
    }

    protected void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e){
                logger.error(e.getMessage());
            }
        }
        closeable = null;
    }

    protected void flushAndClose(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        if (closeable instanceof Flushable){
            try {
                ((Flushable)closeable).flush();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        close(closeable);
    }

    //do not overwrite a finished state.  This should
    //represent the initial cause; all subsequent calls
    //to set will be ignored!!!
    private void setEndedState(STATE cause) {
        synchronized(lock) {
            if (currentState == STATE.NOT_YET_STARTED ||
                    currentState == STATE.ACTIVELY_CONSUMING) {
                currentState = cause;
            }
        }
    }
}
