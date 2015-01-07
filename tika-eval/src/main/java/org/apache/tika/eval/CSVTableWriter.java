package org.apache.tika.eval;


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

    public void init() {
        wrapperThread = new Thread(wrapper);
        wrapperThread.run();
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
        if (wrapper != null) {
            try {
                wrapper.close();
            } catch (IOException e) {
                //log it
            }
        }
    }

    @Override
    public void close() throws IOException {
        wrapper.close();
    }
}
