package org.apache.tika.eval.batch;

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
    private String encoding = "UTF-16LE";
    private Thread printerThread;

    public BasicFileComparerManager(List<FileResourceConsumer> consumers, File outputFile) {
        super(consumers);
        this.outputFile = outputFile;
    }

    @Override
    public void init() {
        try {
            OutputStream os = new FileOutputStream(outputFile);
            //need to include BOM! TODO: parameterize encoding, bom and delimiter
            os.write((byte)255);
            os.write((byte)254);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, encoding));
            CSVPrinter p = new CSVPrinter(writer, CSVFormat.EXCEL.withDelimiter('\t'));
            this.printer = new ThreadSafeCSVWrapper(p, 1000);
            printerThread = new Thread(printer);
            printerThread.start();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (FileResourceConsumer consumer: getConsumers()) {
            ((BasicFileComparer)consumer).setTableWriter(printer);
        }
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
