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
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.tika.batch.BatchProcess;
import org.apache.tika.batch.ParallelFileProcessingResult;
import org.apache.tika.batch.builders.BatchProcessBuilder;
import org.apache.tika.io.IOUtils;

public class FSBatchProcessCLI {
    public static String FINISHED_STRING = "Main thread in TikaFSBatchCLI has finished processing.";

    public Options getOptions() {
        Options options = new Options();
        options.addOption("bc", "batch-config", true, "xml config file");
        options.addOption("randomCrawl", false, "crawl files randomly");
        options.addOption("numConsumers", true, "how many consumer threads to use (default=number of processors-1)");
        options.addOption("maxFileSizeBytes", true, "if an input file is larger than this, skip it.");
        options.addOption("maxStaleConsumers", true, "maximum number of stale consumers to allow before shutdown");
        options.addOption("maxQueueSize", true, "maximum size of the FileResource queue");
        options.addOption("fileList", true, "list of files to process (files relative to srcDir)");
        options.addOption("fileListEncoding", true, "encoding for fileList");
        options.addOption("srcDir", "sourceDirectory", true, "source directory (must be specified!)");
        options.addOption("startDir", "startDirectory", true, "start directory (default: srcDir). Must be a child of or equal to srcDir");
        options.addOption("targDir", "targetDirectory", true, "target directory (must be specified!)");
        options.addOption("recursiveParserWrapper", false, "use the recursive parser wrapper or not (default = false)");
        options.addOption("handleExisting", true, "if a target file already exists, do you want to: overwrite, rename or skip");
        options.addOption("basicHandlerType", true, "what type of content handler: xml, text, html, body");
        options.addOption("targetSuffix", true, "suffix to add to the end of the target file name");
        options.addOption("staleThresholdMillis", true, "how long to wait before determining that a consumer has gone stale");

        //smelly!!!
        //TODO: need to figure out how to specify options from config?
        options.addOption("thisDir", true, "this dir for eval");
        options.addOption("thatDir", true, "that dir for eval");
        options.addOption("outputFile", true, "results file for eval");
        options.addOption("?", "help", false, "this help message");
        return options;
    }

    public static void main(String[] args) throws Exception {
        Options options = new FSBatchProcessCLI().getOptions();

        CommandLineParser cliParser = new GnuParser();
        CommandLine line = cliParser.parse(options, args);

        if (line.hasOption("help")) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("tika filesystem batch", options);
            System.exit(-1);
        }

        Map<String, String> mapArgs = new HashMap<String, String>();
        for (Option option : line.getOptions()) {
            String v = option.getValue();
            if (v == null || v.equals("")) {
                v = "true";
            }
            mapArgs.put(option.getOpt(), v);
        }
        String configFilePath = line.getOptionValue("bc");
        InputStream is = null;
        if (configFilePath != null) {
            File configFile = new File(configFilePath);
            is = new FileInputStream(configFile);
        }
        if (is == null) {
            throw new RuntimeException("Must specify a configuration file: -bc");
        }
        BatchProcessBuilder b = new BatchProcessBuilder();
        BatchProcess process = b.build(is, mapArgs);
        IOUtils.closeQuietly(is);
        ParallelFileProcessingResult result = process.execute();
        System.out.println(FINISHED_STRING);
        System.out.println("\n");
        System.out.println(result.toString());
        System.exit(result.getExitStatus());
    }

}
