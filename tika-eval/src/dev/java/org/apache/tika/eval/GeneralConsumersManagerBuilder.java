package org.apache.tika.eval;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.tika.batch.ConsumersManager;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.batch.builders.AbstractConsumersBuilder;
import org.apache.tika.batch.builders.BatchProcessBuilder;
import org.apache.tika.eval.batch.BasicFileComparerManager;
import org.apache.tika.eval.db.SqliteUtil;
import org.apache.tika.eval.io.CSVTableWriter;
import org.apache.tika.eval.io.JDBCTableWriter;
import org.apache.tika.eval.io.TableWriter;
import org.apache.tika.util.XMLDOMUtil;
import org.w3c.dom.Node;

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

/**
 *
 * This is a toy dev class!!!
 *
 * */
public class GeneralConsumersManagerBuilder extends AbstractConsumersBuilder {

    @Override
    public ConsumersManager build(Node node, Map<String, String> runtimeAttributes, ArrayBlockingQueue<FileResource> queue) {
        List<FileResourceConsumer> consumers = new LinkedList<FileResourceConsumer>();
        int numConsumers = BatchProcessBuilder.getNumConsumers(runtimeAttributes);

        Map<String, String> localAttrs = XMLDOMUtil.mapifyAttrs(node, runtimeAttributes);
        File thisRootDir = getNonNullFile(localAttrs, "thisDir");
        File thatRootDir = getNonNullFile(localAttrs, "thatDir");
        BasicFileComparer.init(thisRootDir, thatRootDir);
        File outputFile = getFile(localAttrs, "outputFile");
        File dbDir = getFile(localAttrs, "dbDir");
        String tableName = localAttrs.get("tableName");
        File langModelDir = getNonNullFile(localAttrs, "langModelDir");

        BasicFileComparer.setLangModelDir(langModelDir);


        TableWriter writer = buildTableWriter(outputFile, dbDir, tableName);

        for (int i = 0; i < numConsumers; i++) {
            FileResourceConsumer consumer = new BasicFileComparer(queue);
            consumers.add(consumer);
        }
        return new BasicFileComparerManager(consumers, writer);
    }


    private TableWriter buildTableWriter(File outputFile, File dbDir, String tableName) {
        if (outputFile != null) {
            return buildCSVWriter(outputFile);
        } else if (dbDir != null && tableName != null) {
            return buildDBWriter(dbDir, tableName);
        }
        throw new RuntimeException("Must specify either an outputFile (csv) or a database directory and table name.");
    }

    private TableWriter buildDBWriter(File dbDir, String tableName) {
        TableWriter writer;
        try {
            writer = new JDBCTableWriter(BasicFileComparer.getHeaders(), new SqliteUtil(), dbDir, tableName);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        return writer;
    }

    private TableWriter buildCSVWriter(File outputFile) {
        TableWriter tableWriter = null;
        try {
            OutputStream os = new FileOutputStream(outputFile);
            //need to include BOM! TODO: parameterize encoding, bom and delimiter
            os.write((byte) 255);
            os.write((byte) 254);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-16LE"));
            CSVPrinter p = new CSVPrinter(writer, CSVFormat.EXCEL.withDelimiter('\t'));


            tableWriter = new CSVTableWriter(p, 1000, BasicFileComparer.getHeaders());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tableWriter;
    }

    private File getNonNullFile(Map<String, String> attrs, String key) {
        File f = getFile(attrs, key);
        if (f == null) {
            throw new RuntimeException("Must specify a file for this attribute: "+key);
        }
        return f;
    }

    private File getFile(Map<String, String> attrs, String key) {
        String filePath = attrs.get(key);
        if (filePath == null) {
            return null;
        }
        return new File(filePath);
    }

}
