package org.apache.tika.eval.batch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.tika.batch.ConsumersManager;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.eval.BasicFileComparer;
import org.apache.tika.eval.ThreadSafeCSVWrapper;


public class BasicFileComparerManager extends ConsumersManager {

    private final File outputFile;
    private ThreadSafeCSVWrapper printer = null;
    private String encoding = "UTF-8";
    private Thread printerThread;

    public BasicFileComparerManager(List<FileResourceConsumer> consumers, File outputFile) {
        super(consumers);
        this.outputFile = outputFile;
    }

    @Override
    public void init() {
        try {
            OutputStream os = new FileOutputStream(outputFile);
            //TODO: parameterize encoding, csvformat and queue size
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, encoding));
            CSVPrinter p = new CSVPrinter(writer, CSVFormat.EXCEL);
            this.printer = new ThreadSafeCSVWrapper(p, 1000);
            printerThread = new Thread(printer);
            System.out.println("about to start");
            printerThread.start();
            System.out.println("started");
            String[] headers = new String[]{
                    "ThisFile", "JsonExceptionThis", "JsonExceptionThat",
                    "ExceptionMsgThis", "ExceptionMsgThat", "FileSuffix",
                    "AttachCountThis", "AttachCountThat",
                    "MetadataKeysThis", "MetadataKeysThat",
                    "ElapsedMillisThis", "ElapsedMillisThat",
                    "TokenCountThis", "TokenCountThat", "TokenOverlap"
            };
            List<String> headerList = Arrays.asList(headers);
            this.printer.printRecord(headerList);
            System.out.println("printed");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (FileResourceConsumer consumer: getConsumers()) {
            ((BasicFileComparer)consumer).setWriter(printer);
        }
        System.out.println("returning from init");

    }

    @Override
    public void shutdown() {

        long totTime = 0;
        while (printer.stillGoing() && totTime < 10000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e){
                break;
            }
            totTime += 100;
        }
        printer.shutdown();
        printerThread.interrupt();
        if (printer != null) {
            try {
                printer.flush();
                printer.close();
            } catch (IOException e) {
                //log it
            }
        }
    }
}
