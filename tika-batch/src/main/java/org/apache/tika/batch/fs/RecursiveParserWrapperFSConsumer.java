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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.batch.OutputStreamFactory;
import org.apache.tika.batch.ParserFactory;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.ContentHandlerFactory;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Basic FileResourceConsumer that reads files from an input
 * directory and writes content to the output directory.
 * <p/>
 * This tries to catch most of the common exceptions, log them and
 * store them in the metadata list output.
 */
public class RecursiveParserWrapperFSConsumer extends FileResourceConsumer {


    private final ParserFactory parserFactory;
    private final ContentHandlerFactory contentHandlerFactory;
    private final OutputStreamFactory fsOSFactory;
    private String outputEncoding = "UTF-8";


    public RecursiveParserWrapperFSConsumer(ArrayBlockingQueue<FileResource> queue,
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

        Parser wrapped = parserFactory.getParser(TikaConfig.getDefaultConfig());
        RecursiveParserWrapper parser = new RecursiveParserWrapper(wrapped, contentHandlerFactory);
        ParseContext context = new ParseContext();

//        if (parseRecursively == true) {
        context.set(Parser.class, parser);
//        }

        //try to open outputstream first
        OutputStream os = null;
        try {
            os = fsOSFactory.getOutputStream(fileResource.getMetadata());
        } catch (IOException e) {
            super.logger.fatal(getLogMsg(fileResource.getResourceId(), e));
            throw new BatchNoRestartError("IOException trying to open output stream for "+
                    fileResource.getResourceId() + " :: " + e.getMessage());
        }
        //os can be null if fsOSFactory is set to skip processing a file and the target
        //file already exists
        if (os == null) {
            super.logger.debug("Skipping: " + fileResource.getMetadata().get(FSProperties.FS_ABSOLUTE_PATH));
            return false;
        }
        String absolutePath = fileResource.getMetadata().get(FSProperties.FS_ABSOLUTE_PATH);

        //try to open the inputstream before the parse.
        //if the parse hangs or throws a nasty exception, at least there will
        //be a zero byte file there so that the batchrunner can skip that problematic
        //file during the next run.
        InputStream is = null;
        try {
            is = fileResource.openInputStream();
        } catch (IOException e) {
            logger.error(getLogMsg(fileResource.getResourceId(), e));
            incrementHandledExceptions();
            flushAndClose(os);
            return false;
        }

        Throwable thrown = null;
        List<Metadata> metadataList = null;
        try {
            parser.parse(is, new DefaultHandler(),
                    fileResource.getMetadata(), context);
            metadataList = parser.getMetadata();
        } catch (Throwable t) {
            thrown = t;
            if (t instanceof Error) {
                logger.fatal(getLogMsg(fileResource.getResourceId(), t));
            } else {
                logger.error(getLogMsg(fileResource.getResourceId(), t));
            }
            if (metadataList == null) {
                metadataList = new LinkedList<Metadata>();
            }
            Metadata m = null;
            if (metadataList.size() > 0) {
                m = metadataList.remove(0);
            } else {
                m = fileResource.getMetadata();
            }
            StringWriter stringWriter = new StringWriter();
            PrintWriter w = new PrintWriter(stringWriter);
            t.printStackTrace(w);
            stringWriter.flush();
            m.add(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX+"runtime", stringWriter.toString());
            metadataList.add(0, m);
        } finally {
            close(is);
        }

        Writer writer = null;

        try {
            writer = new OutputStreamWriter(os, getOutputEncoding());
            JsonMetadataList.toJson(metadataList, writer);
        } catch (Exception e) {
            logger.error(getLogMsg(fileResource.getResourceId() ,e));
        } finally {
            flushAndClose(writer);
        }

        if (thrown != null) {
            if (thrown instanceof Error) {
                throw (Error) thrown;
            } else {
                incrementHandledExceptions();
                return false;
            }
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
