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
package org.apache.tika.cli;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.tika.io.TemporaryResources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TikaCLIBatchCommandLineTest {

    File testInput = null;

    @Before
    public void init() {
        testInput = new File("testInput");
        if (! testInput.mkdirs()) {
            throw new RuntimeException("Failed to open test input directory");
        }
    }

    @After
    public void tearDown() {
        try {
            FileUtils.deleteDirectory(testInput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testJVMOpts() throws Exception {
        TemporaryResources tmp = new TemporaryResources();
        String path = testInput.getAbsolutePath();
        if (path.contains(" ")) {
            path = "\""+path+"\"";
        }
        String[] params = {"-JXmx1g", "-JDlog4j.configuration=batch_process_log4j.xml", path};

        try {
            String[] commandLine = BatchCommandLineBuilder.build(params);
            StringBuilder sb = new StringBuilder();

            for (String s : commandLine) {
                sb.append(s).append(" ");
            }
            String s = sb.toString();
            int classInd = s.indexOf("org.apache.tika.batch.fs.FSBatchProcessCLI");
            int xmx = s.indexOf("-Xmx1g");
            int inputDir = s.indexOf("-inputDir");
            int log = s.indexOf("-Dlog4j.configuration");
            assertTrue(classInd > -1);
            assertTrue(xmx > -1);
            assertTrue(inputDir > -1);
            assertTrue(log > -1);
            assertTrue(xmx < classInd);
            assertTrue(log < classInd);
            assertTrue(inputDir > classInd);
        } finally {
            tmp.close();
        }
    }

    @Test
    public void testBasicMappingOfArgs() throws Exception {
        //TODO: add more tests
        TemporaryResources tmp = new TemporaryResources();
        String path = testInput.getAbsolutePath();
        if (path.contains(" ")) {
            path = "\""+path+"\"";
        }
        String[] params = {"-JXmx1g", "-JDlog4j.configuration=batch_process_log4j.xml",
                "-bc", "batch-config.xml",
                "-J", "-h", path};

        try {
            String[] commandLine = BatchCommandLineBuilder.build(params);
            Map<String, String> attrs = mapify(commandLine);
            assertEquals("true", attrs.get("-recursiveParserWrapper"));
            assertEquals("html", attrs.get("-basicHandlerType"));
            assertEquals("json", attrs.get("-outputSuffix"));
            assertEquals("batch-config.xml", attrs.get("-bc"));

        } finally {
            tmp.close();
        }
    }

    private Map<String, String> mapify(String[] args) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                String k = args[i];
                String v = "";
                if (i < args.length-1 && ! args[i+1].startsWith("-")) {
                    v = args[i+1];
                    i++;
                }
                map.put(k, v);
            }
        }
        return map;
    }

}
