package org.apache.tika.eval;

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
