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
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.tika.extractor.DocumentSelector;
import org.apache.tika.metadata.Metadata;

public abstract class FileResourceCrawler implements Callable<IFileProcessorFutureResult> {

    private volatile boolean hasCompletedCrawling = false;
    private volatile boolean isActive = true;
    private volatile boolean timedOut = false;

    //how long to pause if can't add to queue
    private static final long PAUSE_INCREMENT_MILLIS = 1000;

    protected static Logger logger = Logger.getLogger(FileResourceCrawler.class.toString());

    private int maxFilesToAdd = -1;
    private int maxFilesToConsider = -1;

    private final ArrayBlockingQueue<FileResource> queue;
    private final int numConsumers;


    private long maxConsecWaitInMillis = 300000;//300,000ms = 5 minutes
    private DocumentSelector documentSelector = null;

    //number of files added to queue
    private int added = 0;
    //number of files considered including those that were rejected by documentSelector
    private int considered = 0;

    /**
     * @param queue        shared queue
     * @param numConsumers number of consumers (needs to know how many poisons to add when done)
     */
    public FileResourceCrawler(ArrayBlockingQueue<FileResource> queue, int numConsumers) {
        this.queue = queue;
        this.numConsumers = numConsumers;
    }

    public abstract void start() throws InterruptedException;

    public FileResourceCrawlerFutureResult call() {
        try {
            start();
        } catch (InterruptedException e) {
            //this can be triggered by shutdownNow in BatchProcess
            logger.info("InterruptedException in FileCrawler: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Exception in FileCrawler: " + e.getMessage());
        } finally {
            isActive = false;
        }

        try {
            shutdown();
        } catch (InterruptedException e) {
            //swallow
        }

        return new FileResourceCrawlerFutureResult(considered, added);
    }

    protected boolean tryToAdd(FileResource fileResource) throws InterruptedException {

        if (maxFilesToAdd > -1 && added >= maxFilesToAdd) {
            shutdown();
        }

        boolean isAdded = false;
        if (select(fileResource.getMetadata())) {
            long totalConsecutiveWait = 0;
            while (queue.offer(fileResource, 1L, TimeUnit.SECONDS) == false) {

                logger.info("FileResourceCrawler is pausing.  Queue is full: " + queue.size());
                Thread.sleep(PAUSE_INCREMENT_MILLIS);
                totalConsecutiveWait += PAUSE_INCREMENT_MILLIS;
                if (maxConsecWaitInMillis > -1 && totalConsecutiveWait > maxConsecWaitInMillis) {
                    timedOut = true;
                    logger.error("Crawler had to wait longer than max consecutive wait time.");
                    throw new InterruptedException("FileResourceCrawler had to wait longer than max consecutive wait time.");
                }
                if (Thread.currentThread().isInterrupted()) {
                    logger.info("FileResourceCrawler shutting down because of interrupted thread.");
                    throw new InterruptedException("FileResourceCrawler interrupted.");
                }
            }
            isAdded = true;
            added++;
        } else {
            logger.debug("crawler did not select: "+fileResource.getResourceId());
        }
        considered++;
        return isAdded;
    }

    private void shutdown() throws InterruptedException {

        if (hasCompletedCrawling == true) {
            return;
        }
        int i = 0;
        long start = new Date().getTime();
        while (queue.offer(new PoisonFileResource(), 1L, TimeUnit.SECONDS)) {
            long elapsed = new Date().getTime() - start;
            if (maxConsecWaitInMillis > -1 && elapsed > maxConsecWaitInMillis) {
                logger.error("Crawler timed out while trying to add poison");
                throw new InterruptedException("FileResourceCrawler timed out while trying to shutdown.");
            }
            if (i++ >= numConsumers) {
                break;
            }

        }
        hasCompletedCrawling = true;
    }

    /**
     * If the crawler stops for any reason, it is no longer active.
     *
     * @return whether crawler is active or not
     */
    public boolean isActive() {
        return isActive;
    }

    public void setMaxConsecWaitInMillis(long maxConsecWaitInMillis) {
        this.maxConsecWaitInMillis = maxConsecWaitInMillis;
    }
    public void setDocumentSelector(DocumentSelector documentSelector) {
        this.documentSelector = documentSelector;
    }

    public int getConsidered() {
        return considered;
    }

    protected boolean select(Metadata m) {
        return documentSelector.select(m);
    }

    /**
     * Maximum number of files to add.  If {@link #maxFilesToAdd} < 0 (default),
     * then this crawler will add all documents.
     *
     * @param maxFilesToAdd
     */
    public void setMaxFilesToAdd(int maxFilesToAdd) {
        this.maxFilesToAdd = maxFilesToAdd;
    }


    /**
     * Maximum number of files to consider.  A file is considered
     * whether or not the DocumentSelector selects a document.
     * <p/>
     * If {@link #maxFilesToConsider} < 0 (default), then this crawler
     * will add all documents.
     *
     * @param maxFilesToConsider
     */
    public void setMaxFilesToConsider(int maxFilesToConsider) {
        this.maxFilesToConsider = maxFilesToConsider;
    }

    /**
     * Use sparingly.  This synchronizes on the queue!
     * @return whether this queue contains any non-poison file resources
     */
    public boolean isQueueEmpty() {
        int size= 0;
        synchronized(queue) {
            Iterator<FileResource> it = queue.iterator();
            while (it.hasNext()) {
                if (it.next() instanceof PoisonFileResource) {
                } else {
                    size++;
                }
            }
        }
        return size == 0;
    }

    /**
     * Returns whether the crawler timed out while trying to add a resource
     * to the queue.
     * <p/>
     * If the crawler timed out while trying to add poison, this is not
     * set to true.
     *
     * @return
     */
    public boolean wasTimedOut() {
        return timedOut;
    }

    public int getAdded() {
        return added;
    }
}