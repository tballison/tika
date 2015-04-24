package org.apache.tika.batch.testutils;

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

import static junit.framework.TestCase.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tika.batch.fs.StringStreamGobbler;

public class BatchProcessTestExecutor {
    private final String processClass;
    private final Map<String, String> args;
    private final String configPath;
    private int exitValue = Integer.MIN_VALUE;

    public BatchProcessTestExecutor(String processClass, Map<String, String> args) {
        this(processClass, args, "/tika-batch-config-test.xml");
    }

    public BatchProcessTestExecutor(String processClass, Map<String, String> args, String configPath) {
        this.args = args;
        this.configPath = configPath;
        this.processClass = processClass;
    }

    public StreamStrings execute() {
        Process p = null;
        try {
            ProcessBuilder b = getNewBatchRunnerProcess(processClass, configPath, args);
            p = b.start();
            StringStreamGobbler errorGobbler = new StringStreamGobbler(p.getErrorStream());
            StringStreamGobbler outGobbler = new StringStreamGobbler(p.getInputStream());
            Thread errorThread = new Thread(errorGobbler);
            Thread outThread = new Thread(outGobbler);
            errorThread.start();
            outThread.start();
            while (true) {
                try {
                    exitValue = p.exitValue();
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

    public int getExitValue() {
        return exitValue;
    }

    public ProcessBuilder getNewBatchRunnerProcess(String processClass, String testConfig, Map<String, String> args) {
        List<String> argList = new ArrayList<String>();
        for (Map.Entry<String, String> e : args.entrySet()) {
            argList.add("-"+e.getKey());
            argList.add(e.getValue());
        }

        String[] fullCommandLine = commandLine(processClass, testConfig, argList.toArray(new String[argList.size()]));
        return new ProcessBuilder(fullCommandLine);
    }

    private String[] commandLine(String processClass, String testConfig, String[] args) {
        List<String> commandLine = new ArrayList<String>();
        commandLine.add("java");
        commandLine.add("-Dlog4j.configuration=file:"+
                this.getClass().getResource("/log4j_process.properties").getFile());
        commandLine.add("-Xmx128m");
        commandLine.add("-cp");
        String cp = System.getProperty("java.class.path");
        //need to test for " " on *nix, can't just add double quotes
        //across platforms.
        if (cp.contains(" ")){
            cp = "\""+cp+"\"";
        }
        commandLine.add(cp);
        commandLine.add(processClass);//"org.apache.tika.batch.fs.FSBatchProcessCLI");

        String configFile = this.getClass().getResource(testConfig).getFile();
        commandLine.add("-bc");

        commandLine.add(configFile);

        for (String s : args) {
            commandLine.add(s);
        }
        return commandLine.toArray(new String[commandLine.size()]);
    }

    private void destroyProcess(Process p) {
        if (p == null)
            return;

        try {
            p.exitValue();
        } catch (IllegalThreadStateException e) {
            p.destroy();
        }
    }
}

