package org.apache.tika.eval;
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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVPrinter;

public class ThreadSafeCSVWrapper implements Runnable {

    private final ArrayBlockingQueue<List<String>> queue;
    private final CSVPrinter printer;
    private volatile boolean keepGoing = true;

    public ThreadSafeCSVWrapper(CSVPrinter printer, int queueSize) {
        queue = new ArrayBlockingQueue<List<String>>(queueSize);
        this.printer = printer;
    }

    @Override
    public void run() {
        while (keepGoing) {
            try {
                List<String> row = queue.poll(100, TimeUnit.MILLISECONDS);
                if (row != null) {
                    printer.printRecord(row);
                }
            } catch (InterruptedException e) {
                keepGoing = false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public void printRecord(List<String> row) {
        try {
            //block
            queue.put(row);
        } catch (InterruptedException e) {
            keepGoing = false;
            //TODO: log this.  This shouldn't happen under
            //normal circumpstances
        }
    }

    public void shutdown() {
        keepGoing = false;
    }

    public boolean stillGoing() {
        return queue.size() > 0;
    }
    public void flush() throws IOException {
        printer.flush();
    }

    public void close() throws IOException {
        printer.close();
    }
}
