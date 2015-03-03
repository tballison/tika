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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;

import org.apache.tika.batch.BatchProcess;
import org.apache.tika.batch.ParallelFileProcessingResult;
import org.junit.Test;

public class HandlerBuilderTest extends FSBatchTestBase {

    @Test
    public void testXML() throws Exception {

        File outputDir = getNewOutputDir("handler-xml-");
        Map<String, String> args = getDefaultArgs("basic", outputDir);
        args.put("basicHandlerType", "xml");
        args.put("outputSuffix", "xml");

        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        ParallelFileProcessingResult result = runner.execute();
        File outputFile = new File(outputDir, "test1.txt.xml");
        String resultString = getStringFromFile(outputFile, "UTF-8");
        assertTrue(resultString.contains("<html xmlns=\"http://www.w3.org/1999/xhtml\">"));
        assertTrue(resultString.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assertTrue(resultString.contains("This is tika-batch's first test file"));
    }


    @Test
    public void testHTML() throws Exception {
        File outputDir = getNewOutputDir("handler-html-");

        Map<String, String> args = getDefaultArgs("basic", outputDir);
        args.put("basicHandlerType", "html");
        args.put("outputSuffix", "html");
        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        ParallelFileProcessingResult result = runner.execute();
        File outputFile = new File(outputDir, "test1.txt.html");
        String resultString = getStringFromFile(outputFile, "UTF-8");
        assertTrue(resultString.contains("<html xmlns=\"http://www.w3.org/1999/xhtml\">"));
        assertFalse(resultString.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assertTrue(resultString.contains("This is tika-batch's first test file"));
    }

    @Test
    public void testText() throws Exception {
        File outputDir = getNewOutputDir("handler-txt-");

        Map<String, String> args = getDefaultArgs("basic", outputDir);
        args.put("basicHandlerType", "txt");
        args.put("outputSuffix", "txt");

        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        ParallelFileProcessingResult result = runner.execute();
        File outputFile = new File(outputDir, "test1.txt.txt");
        String resultString = getStringFromFile(outputFile, "UTF-8");
        assertFalse(resultString.contains("<html xmlns=\"http://www.w3.org/1999/xhtml\">"));
        assertFalse(resultString.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assertTrue(resultString.contains("This is tika-batch's first test file"));
    }


    @Test
    public void testXMLWithWriteLimit() throws Exception {
        File outputDir = getNewOutputDir("handler-xml-write-limit-");

        Map<String, String> args = getDefaultArgs("basic", outputDir);
        args.put("writeLimit", "5");

        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        ParallelFileProcessingResult result = runner.execute();

        File outputFile = new File(outputDir, "test1.txt.xml");
        String resultString = getStringFromFile(outputFile, "UTF-8");
        //this is not ideal. How can we change handlers to writeout whatever
        //they've gotten so far, up to the writeLimit?
        assertTrue(resultString.equals(""));
    }

    @Test
    public void testRecursiveParserWrapper() throws Exception {
        File outputDir = getNewOutputDir("handler-recursive-parser");

        Map<String, String> args = getDefaultArgs("basic", outputDir);
        args.put("basicHandlerType", "txt");
        args.put("outputSuffix", "json");
        args.put("recursiveParserWrapper", "true");

        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        ParallelFileProcessingResult result = runner.execute();
        File outputFile = new File(outputDir, "test1.txt.json");
        String resultString = getStringFromFile(outputFile, "UTF-8");
        assertTrue(resultString.contains("\"Content-Encoding\":\"ISO-8859-1\""));
        assertTrue(resultString.contains("tika-batch\\u0027s first test file"));
    }

}
