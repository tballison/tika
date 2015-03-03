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


import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.tika.batch.BatchProcess;
import org.junit.Test;

public class BatchProcessTest extends FSBatchTestBase {
    @Test(timeout = 15000)
    public void oneHeavyHangTest() throws Exception {

        File outputDir = getNewOutputDir("one_heavy_hang-");

        Map<String, String> args = getDefaultArgs("one_heavy_hang", outputDir);
        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);
        StreamStrings streamStrings = ex.execute();
        assertEquals(5, outputDir.listFiles().length);
        File hvyHang = new File(outputDir, "hang_heavy_load1.evil.xml");
        assertTrue(hvyHang.exists());
        assertEquals(0, hvyHang.length());
        assertNotContained(BatchProcess.BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString(),
                streamStrings.getErrString());
    }


    @Test(timeout = 15000)
    public void allHeavyHangsTest() throws Exception {
        //each of the three threads hits a heavy hang.  The BatchProcess runs into
        //all timedouts and shuts down.
        File outputDir = getNewOutputDir("allHeavyHangs-");
        Map<String, String> args = getDefaultArgs("heavy_heavy_hangs", outputDir);
        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);
        StreamStrings streamStrings = ex.execute();

        assertEquals(3, outputDir.listFiles().length);
        for (int i = 1; i < 4; i++) {
            File hvyHang = new File(outputDir, "hang_heavy_load" + i + ".evil.xml");
            assertTrue(hvyHang.exists());
            assertEquals(0, hvyHang.length());
        }
        assertContains(BatchProcess.BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString(),
                streamStrings.getErrString());
    }

    @Test(timeout = 30000)
    public void allHeavyHangsTestWithCrazyNumberConsumersTest() throws Exception {
        File outputDir = getNewOutputDir("allHeavyHangsCrazyNumberConsumers-");
        Map<String, String> args = getDefaultArgs("heavy_heavy_hangs", outputDir);
        args.put("numConsumers", "100");
        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);
        StreamStrings streamStrings = ex.execute();
        assertEquals(6, outputDir.listFiles().length);

        for (int i = 1; i < 6; i++){
            File hvyHang = new File(outputDir, "hang_heavy_load"+i+".evil.xml");
            assertTrue(hvyHang.exists());
            assertEquals(0, hvyHang.length());
        }
        assertContains(new File(outputDir, "test1.txt.xml"), "UTF-8", "This is tika-batch's first test file");
        assertNotContained(BatchProcess.BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString(),
                streamStrings.getErrString());
    }

    @Test(timeout = 30000)
    public void allHeavyHangsTestWithStarvedCrawler() throws Exception {
        //this tests that if all consumers are hung and the crawler is
        //waiting to add to the queue, there isn't deadlock.  The batchrunner should
        //shutdown and ask to be restarted.
        File outputDir = getNewOutputDir("allHeavyHangsStarvedCrawler-");
        Map<String, String> args = getDefaultArgs("heavy_heavy_hangs", outputDir);
        args.put("numConsumers", "2");
        args.put("maxQueueSize", "2");
        args.put("timeoutThresholdMillis", "100000000");//make sure that the batch process doesn't time out
        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);
        StreamStrings streamStrings = ex.execute();

        assertEquals(2, outputDir.listFiles().length);

        for (int i = 1; i < 2; i++){
            File hvyHang = new File(outputDir, "hang_heavy_load"+i+".evil.xml");
            assertTrue(hvyHang.exists());
            assertEquals(0, hvyHang.length());
        }
        assertContains(BatchProcess.BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString(),
                streamStrings.getErrString());
        assertContains("Crawler timed out", streamStrings.getErrString());
    }

    @Test(timeout = 15000)
    public void outOfMemory() throws Exception {
        //the first consumer should sleep for 10 seconds
        //the second should be tied up in a heavy hang
        //the third one should hit the oom after processing test1.txt
        //no consumers should process test2-4.txt!
        File outputDir = getNewOutputDir("oom-");

        Map<String, String> args = getDefaultArgs("oom", outputDir);
        args.put("numConsumers", "3");
        args.put("timeoutThresholdMillis", "30000");

        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);
        StreamStrings streamStrings = ex.execute();

        assertEquals(4, outputDir.listFiles().length);
        assertContains(new File(outputDir, "test1.txt.xml"), "UTF-8", "This is tika-batch's first test file");
        assertContains(BatchProcess.BATCH_CONSTANTS.BATCH_PROCESS_FATAL_MUST_RESTART.toString(),
                streamStrings.getErrString());
    }



    @Test(timeout = 15000)
    public void noRestart() throws Exception {
        File outputDir = getNewOutputDir("no_restart");

        Map<String, String> args = getDefaultArgs("no_restart", outputDir);
        args.put("numConsumers", "1");

        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);

        StreamStrings streamStrings = ex.execute();
        File[] files = outputDir.listFiles();
        assertEquals(2, files.length);
        assertEquals(0, files[1].length());
        assertContains("exitStatus=1", streamStrings.getOutString());
        assertContains("causeForTermination='MAIN_LOOP_EXCEPTION_NO_RESTART'",
                streamStrings.getOutString());
    }

    /**
     * This tests to make sure that BatchProcess waits the appropriate
     * amount of time on an early termination before stopping.
     *
     * If this fails, then interruptible parsers (e.g. those with
     * nio channels) will be interrupted and there will be corrupted data.
     */
    @Test(timeout = 60000)
    public void testWaitAfterEarlyTermination() throws Exception {
        File outputDir = getNewOutputDir("wait_after_early_termination");

        Map<String, String> args = getDefaultArgs("wait_after_early_termination", outputDir);
        args.put("numConsumers", "1");
        args.put("maxAliveTimeSeconds", "5");//main process loop should stop after 5 seconds
        args.put("timeoutThresholdMillis", "300000");//effectively never
        args.put("pauseOnEarlyTerminationMillis", "20000");//let the parser have up to 20 seconds

        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);

        StreamStrings streamStrings = ex.execute();
        File[] files = outputDir.listFiles();
        assertEquals(1, files.length);
        assertContains(files[0], "UTF-8", "type=\"sleep\" max_millis=\"10000\"");
        assertContains("exitStatus=-1", streamStrings.getOutString());
        assertContains("causeForTermination='BATCH_PROCESS_ALIVE_TOO_LONG'",
                streamStrings.getOutString());
    }

    @Test(timeout = 60000)
    public void testTimeOutAfterBeingAskedToShutdown() throws Exception {
        File outputDir = getNewOutputDir("timeout_after_early_termination");

        Map<String, String> args = getDefaultArgs("timeout_after_early_termination", outputDir);
        args.put("numConsumers", "1");
        args.put("maxAliveTimeSeconds", "5");//main process loop should stop after 5 seconds
        args.put("timeoutThresholdMillis", "10000");
        args.put("pauseOnEarlyTerminationMillis", "20000");//let the parser have up to 20 seconds

        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(args);
        StreamStrings streamStrings = ex.execute();
        File[] files = outputDir.listFiles();
        assertEquals(1, files.length);
        assertEquals(0, files[0].length());
        assertContains("exitStatus=-1", streamStrings.getOutString());
        assertContains("causeForTermination='BATCH_PROCESS_ALIVE_TOO_LONG'",
                streamStrings.getOutString());
    }

    private class BatchProcessTestExecutor {
        private final Map<String, String> args;

        public BatchProcessTestExecutor(Map<String, String> args) {
            this.args = args;
        }

        private StreamStrings execute() {
            Process p = null;
            try {
                ProcessBuilder b = getNewBatchRunnerProcess("/tika-batch-config-evil-test.xml", args);
                p = b.start();
                StringStreamGobbler errorGobbler = new StringStreamGobbler(p.getErrorStream());
                StringStreamGobbler outGobbler = new StringStreamGobbler(p.getInputStream());
                Thread errorThread = new Thread(errorGobbler);
                Thread outThread = new Thread(outGobbler);
                errorThread.start();
                outThread.start();
                while (true) {
                    try {
                        p.exitValue();
                        break;
                    } catch (IllegalThreadStateException e) {
                        //still going;
                    }
                }
                errorGobbler.stopGobblingAndDie();
                outGobbler.stopGobblingAndDie();
                errorThread.interrupt();
                outThread.interrupt();
                return new StreamStrings(outGobbler.toString(), errorGobbler.toString());
            } catch (IOException e) {
                fail();
            } finally {
                destroyProcess(p);
            }
            return null;
        }

    }

    private class StreamStrings {
        private final String outString;
        private final String errString;

        private StreamStrings(String outString, String errString) {
            this.outString = outString;
            this.errString = errString;
        }

        private String getOutString() {
            return outString;
        }

        private String getErrString() {
            return errString;
        }

        @Override
        public String toString() {
            return "OUT>>"+outString+"<<\n"+
                    "ERR>>"+errString+"<<\n";
        }
    }
}
