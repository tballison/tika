package org.apache.tika.batch.fs;

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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.batch.OutputStreamFactory;
import org.apache.tika.batch.ParserFactory;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ContentHandlerFactory;
import org.xml.sax.ContentHandler;

/**
 * Basic FileResourceConsumer that reads files from an input
 * directory and writes content to the output directory.
 * <p>
 * This catches all exceptions and errors and then logs them.
 * This will re-throw errors.
 *
 */
public class BasicTikaFSConsumer extends FileResourceConsumer {

    private boolean parseRecursively = true;
    private final ParserFactory parserFactory;
    private final ContentHandlerFactory contentHandlerFactory;
    private final OutputStreamFactory fsOSFactory;
    private String outputEncoding = "UTF-8";


    public BasicTikaFSConsumer(ArrayBlockingQueue<FileResource> queue,
                               ParserFactory parserFactory,
                               ContentHandlerFactory contentHandlerFactory,
                               OutputStreamFactory fsOSFactory) {
        super(queue);
        this.parserFactory = parserFactory;
        this.contentHandlerFactory = contentHandlerFactory;
        this.fsOSFactory = fsOSFactory;
    }

    @Override
    public boolean processFileResource(FileResource fileResource) {

        Parser parser = parserFactory.getParser(TikaConfig.getDefaultConfig());
        ParseContext context = new ParseContext();
        if (parseRecursively == true) {
            context.set(Parser.class, parser);
        }

        OutputStream os = null;
        try {
            os = fsOSFactory.getOutputStream(fileResource.getMetadata());
        } catch (IOException e) {
            logger.fatal(getLogMsg(fileResource.getResourceId(), e));
            flushAndClose(os);
            throw new BatchNoRestartError("IOException trying to open output stream for "+
                    fileResource.getResourceId() + " :: " + e.getMessage());
        }
        //os can be null if fsOSFactory is set to skip processing a file if the output
        //file already exists
        if (os == null) {
            logger.debug("Skipping: " + fileResource.getMetadata().get(FSProperties.FS_ABSOLUTE_PATH));
            return false;
        }

        InputStream is = null;
        try {
            is = fileResource.openInputStream();
        } catch (IOException e) {
            incrementHandledExceptions();
            logger.error(getLogMsg(fileResource.getResourceId(), e));
            flushAndClose(os);
            return false;
        }

        ContentHandler handler;
        try {
            handler = contentHandlerFactory.getNewContentHandler(os, getOutputEncoding());
        } catch (UnsupportedEncodingException e) {
            close(is);
            flushAndClose(os);
            logger.fatal(getLogMsg(fileResource.getResourceId(), e));
            throw new RuntimeException(e.getMessage());
        }

        //now actually call parse!
        Throwable thrown = null;
        try {
            parser.parse(is, handler,
                    fileResource.getMetadata(), context);
        } catch (Throwable t) {
            logger.error(getLogMsg(fileResource.getResourceId(), t));
            thrown = t;
        } finally {
            close(is);
            flushAndClose(os);
        }

        if (thrown != null) {
            if (thrown instanceof Error) {
                throw (Error)thrown;
            } else {
                incrementHandledExceptions();
            }
            return false;
        }
        return true;
    }

    public String getOutputEncoding() {
        return outputEncoding;
    }

    public void setOutputEncoding(String outputEncoding) {
        this.outputEncoding = outputEncoding;
    }
}
