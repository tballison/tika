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
package org.apache.tika.eval;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.tika.batch.fs.FSBatchProcessCLI;
import org.apache.tika.eval.reports.ResultsReporter;
import org.h2.tools.Console;

public class TikaEvalCLI {
    static final String[] tools = {"Profile", "Compare", "Report", "StartDB"};

    private static String specifyTools() {
        StringBuilder sb = new StringBuilder();
        sb.append("Must specify one of the following tools in the first parameter:\n");
        for (String s : tools) {
            sb.append(s+"\n");
        }
        return sb.toString();

    }

    private void execute(String[] args) throws Exception {
        String tool = args[0];
        String[] subsetArgs = new String[args.length-1];
        System.arraycopy(args, 1, subsetArgs, 0, args.length - 1);
        if (tool.equals("Report")) {
            handleReport(subsetArgs);
        } else if (tool.equals("Compare")) {
            handleCompare(subsetArgs);
        } else if (tool.equals("Profile")) {
            handleProfile(subsetArgs);
        } else if (tool.equals("StartDB")) {
            handleStartDB(subsetArgs);
        } else {
            throw new RuntimeException(specifyTools());
        }
    }

    private void handleStartDB(String[] args) throws SQLException {
        List<String> argList = new ArrayList<>();
        argList.add("-web");
        Console.main(argList.toArray(new String[argList.size()]));
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e){
                break;
            }
        }
    }

    private void handleProfile(String[] subsetArgs) throws Exception {
        boolean containsBC = false;
        //confirm there's a batch-config file
        for (int i = 0; i < subsetArgs.length; i++) {
            if (subsetArgs[i].equals("-bc")) {
                containsBC = true;
                break;
            }
        }

        Path tmpBCConfig = null;
        try {
            tmpBCConfig = Files.createTempFile("tika-eval-profiler", ".xml");
            if (! containsBC) {
                List<String> argList = new ArrayList(Arrays.asList(subsetArgs));
                Files.copy(
                        this.getClass().getResourceAsStream("/tika-eval-profiler-config.xml"),
                        tmpBCConfig, StandardCopyOption.REPLACE_EXISTING);
                argList.add("-bc");
                argList.add(tmpBCConfig.toAbsolutePath().toString());
                subsetArgs = argList.toArray(new String[argList.size()]);
            }
            FSBatchProcessCLI.main(subsetArgs);
        } finally {
            if (tmpBCConfig != null && Files.isRegularFile(tmpBCConfig)) {
                Files.delete(tmpBCConfig);
            }
        }
    }

    private void handleCompare(String[] subsetArgs) throws Exception{
        boolean containsBC = false;
        //confirm there's a batch-config file
        for (int i = 0; i < subsetArgs.length; i++) {
            if (subsetArgs[i].equals("-bc")) {
                containsBC = true;
                break;
            }
        }

        Path tmpBCConfig = null;
        try {
            tmpBCConfig = Files.createTempFile("tika-eval", ".xml");
            if (! containsBC) {
                List<String> argList = new ArrayList(Arrays.asList(subsetArgs));
                Files.copy(
                        this.getClass().getResourceAsStream("/tika-eval-comparison-config.xml"),
                        tmpBCConfig, StandardCopyOption.REPLACE_EXISTING);
                argList.add("-bc");
                argList.add(tmpBCConfig.toAbsolutePath().toString());
                subsetArgs = argList.toArray(new String[argList.size()]);
            }
            FSBatchProcessCLI.main(subsetArgs);
        } finally {
            if (tmpBCConfig != null && Files.isRegularFile(tmpBCConfig)) {
                Files.delete(tmpBCConfig);
            }
        }
    }

    private void handleReport(String[] subsetArgs) throws Exception {
        ResultsReporter.main(subsetArgs);
    }

    public static void main(String[] args) throws Exception {
        TikaEvalCLI cli = new TikaEvalCLI();
        if (args.length == 0) {
            throw new RuntimeException(specifyTools());
        }
        cli.execute(args);
    }
}
