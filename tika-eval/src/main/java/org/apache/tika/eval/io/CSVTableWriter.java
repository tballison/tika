package org.apache.tika.eval.io;


import org.apache.commons.csv.CSVPrinter;
import org.apache.tika.eval.db.ColInfo;

import java.io.IOException;
import java.util.Map;

public class CSVTableWriter implements TableWriter {

    private final ThreadSafeCSVWrapper wrapper;
    private Thread wrapperThread;

    public CSVTableWriter(CSVPrinter printer, int queueSize, Map<String, ColInfo> colInfos) {
        wrapper = new ThreadSafeCSVWrapper(printer, queueSize, colInfos);
    }

    @Override
    public void init() {
        wrapperThread = new Thread(wrapper);
        wrapperThread.start();
    }

    @Override
    public void writeHeaders() throws IOException {
        wrapper.writeHeaders();
    }

    @Override
    public void writeRow(Map<String, String> data) throws IOException {
        wrapper.writeRow(data);
    }

    @Override
    public void shutdown() {
        long totTime = 0;
        while (wrapper.stillGoing() && totTime < 10000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e){
                break;
            }
            totTime += 100;
        }
        wrapper.shutdown();
        wrapperThread.interrupt();
    }

    @Override
    public void close() throws IOException {
        wrapper.close();
    }
}