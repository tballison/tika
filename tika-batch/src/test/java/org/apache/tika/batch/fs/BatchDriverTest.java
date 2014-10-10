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
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.batch.BatchProcessDriverCLI;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BatchDriverTest extends FSBatchTestBase {

    @Test(timeout = 15000)
    public void oneHeavyHangTest() throws Exception {
        //batch runner hits one heavy hang file, keep going
        File targDir = getNewTargDir("daemon-");
        assertNotNull(targDir.listFiles());
        //make sure target directory is empty!
        assertEquals(0, targDir.listFiles().length);

        String[] args = getDefaultCommandLineArgsArr("one_heavy_hang", targDir, null);
        BatchProcessDriverCLI driver = getNewDriver("/tika-batch-config-evil-test.xml", args);
        driver.execute();
        assertEquals(0, driver.getNumRestarts());
        assertFalse(driver.getStoppedSelf());
        assertEquals(5, targDir.listFiles().length);
        assertContains(new File(targDir, "test1.txt.xml"), "UTF-8", "first test file");

    }

    @Test(timeout = 15000)
    public void restartOnFullHangTest() throws Exception {
        //batch runner hits more heavy hangs than threads; needs to restart
        File targDir = getNewTargDir("daemon-");

        //make sure target directory is empty!
        assertEquals(0, targDir.listFiles().length);

        String[] args = getDefaultCommandLineArgsArr("heavy_heavy_hangs", targDir, null);
        BatchProcessDriverCLI driver = getNewDriver("/tika-batch-config-evil-test.xml", args);
        driver.execute();
        //could be one or two depending on timing
        assertTrue(driver.getNumRestarts() > 0);
        assertFalse(driver.getStoppedSelf());
        assertContains(new File(targDir, "test1.txt.xml"), "UTF-8",
                "first test file");
    }

    @Test(timeout = 15000)
    public void restartOnOOMTest() throws Exception {
        //batch runner hits more heavy hangs than threads; needs to restart
        File targDir = getNewTargDir("daemon-");

        //make sure target directory is empty!
        assertEquals(0, targDir.listFiles().length);

        String[] args = getDefaultCommandLineArgsArr("oom", targDir, null);
        BatchProcessDriverCLI driver = getNewDriver("/tika-batch-config-evil-test.xml", args);
        driver.execute();
        assertEquals(1, driver.getNumRestarts());
        assertFalse(driver.getStoppedSelf());
        assertContains(new File(targDir, "test4.txt.xml"),
                "UTF-8", "first test file");
    }

    @Test(timeout = 30000)
    public void allHeavyHangsTestWithStarvedCrawler() throws Exception {
        //this tests that if all consumers are hung and the crawler is
        //waiting to add to the queue, there isn't deadlock.  The BatchProcess should
        //just shutdown, and the driver should restart
        File targDir = getNewTargDir("allHeavyHangsStarvedCrawler-");
        Map<String, String> args = new HashMap<String,String>();
        args.put("-numConsumers", "2");
        args.put("-maxQueueSize", "50");
        args.put("-maxStaleConsumers", "100");
        String[] commandLine = getDefaultCommandLineArgsArr("heavy_heavy_hangs", targDir, args);
        BatchProcessDriverCLI driver = getNewDriver("/tika-batch-config-evil-test.xml", commandLine);
        driver.execute();
        assertEquals(2, driver.getNumRestarts());
        assertFalse(driver.getStoppedSelf());
        assertContains(new File(targDir, "test1.txt.xml"), "UTF-8",
                "first test file");
    }
}
