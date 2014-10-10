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

import java.io.File;
import java.util.Map;

import org.apache.tika.batch.BatchProcess;
import org.apache.tika.batch.ParallelFileProcessingResult;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class OutputStreamFactoryTest extends FSBatchTestBase {


    @Test
    public void testIllegalState() throws Exception {
        File targDir = getNewTargDir("os-factory-illegal-state-");
        Map<String, String> args = getDefaultArgs("basic", targDir);
        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        runner.execute();
        assertEquals(1, targDir.listFiles().length);

        boolean illegalState = false;
        try{
            ParallelFileProcessingResult result = runner.execute();
        } catch (IllegalStateException e) {
            illegalState = true;
        }
        assertTrue(illegalState);
    }

    @Test
    public void testSkip() throws Exception {
        File targDir = getNewTargDir("os-factory-skip-");
        Map<String, String> args = getDefaultArgs("basic", targDir);
        args.put("handleExisting", "skip");
        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        ParallelFileProcessingResult result = runner.execute();
        assertEquals(1, targDir.listFiles().length);

        runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        result = runner.execute();
        assertEquals(1, targDir.listFiles().length);
    }

    @Test
    public void testRename() throws Exception {
        File targDir = getNewTargDir("os-factory-rename-");
        Map<String, String> args = getDefaultArgs("basic", targDir);

        args.put("handleExisting", "rename");
        BatchProcess runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        ParallelFileProcessingResult result = runner.execute();
        assertEquals(1, targDir.listFiles().length);

        runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        result = runner.execute();
        assertEquals(2, targDir.listFiles().length);

        runner = getNewBatchRunner("/tika-batch-config-basic-test.xml", args);
        result = runner.execute();
        assertEquals(3, targDir.listFiles().length);

        int hits = 0;
        for (File f : targDir.listFiles()){
            String name = f.getName();
            if (name.equals("test1.txt.xml")) {
                hits++;
            } else if (name.equals("test1(1).txt.xml")) {
                hits++;
            } else if (name.equals("test1(2).txt.xml")) {
                hits++;
            }
        }
        assertEquals(3, hits);
    }

}
