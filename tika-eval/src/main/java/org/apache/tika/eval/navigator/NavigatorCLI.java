package org.apache.tika.eval.navigator;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.cxf.binding.BindingFactoryManager;
import org.apache.cxf.jaxrs.JAXRSBindingFactory;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.lifecycle.ResourceProvider;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;
import org.apache.tika.Tika;

public class NavigatorCLI {
    private static final Log logger = LogFactory.getLog(NavigatorCLI.class);
    public static final int DEFAULT_PORT = 9998;
    public static final String DEFAULT_HOST = "localhost";
    public static final Set<String> LOG_LEVELS =
            new HashSet<String>(Arrays.asList("debug", "info"));

    private static Options getOptions() {
        Options options = new Options();
        options.addOption("h", "host", true, "host name (default = " + DEFAULT_HOST + ')');
        options.addOption("p", "port", true, "listen port (default = " + DEFAULT_PORT + ')');
        options.addOption("l", "log", true, "request URI log level ('debug' or 'info')");
        options.addOption("s", "srcDocumentRoot", true, "directory root for source documents");
        options.addOption("t", "targetDocumentRoot", true, "directory root for extracted documents");
        options.addOption("?", "help", false, "this help message");

        return options;
    }

    public static void main(String[] args) {

        logger.info("Starting " + new Tika().toString() + " navigator server");

        try {
            Options options = getOptions();

            CommandLineParser cliParser = new GnuParser();
            CommandLine line = cliParser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("tika-eval-navigator", options);
                System.exit(-1);
            }

            String host = DEFAULT_HOST;

            if (line.hasOption("host")) {
                host = line.getOptionValue("host");
            }

            int port = DEFAULT_PORT;

            if (line.hasOption("port")) {
                port = Integer.valueOf(line.getOptionValue("port"));
            }

            File srcDocumentRoot = null;
            if (line.hasOption("srcDocumentRoot")) {
                srcDocumentRoot = new File(line.getOptionValue("srcDocumentRoot"));
            } else {
                System.err.println("Must specify 'srcDocumentRoot'");
                System.exit(-1);
            }

            File targetDocumentRoot = null;
            if (line.hasOption("targetDocumentRoot")) {
                targetDocumentRoot = new File(line.getOptionValue("targetDocumentRoot"));
            } else {
                System.err.println("Must specify 'targetDocumentRoot'");
                System.exit(-1);
            }

            JAXRSServerFactoryBean sf = new JAXRSServerFactoryBean();

            List<ResourceProvider> rCoreProviders = new ArrayList<ResourceProvider>();
            rCoreProviders.add(new SingletonResourceProvider(new JSONViewer(targetDocumentRoot)));
            rCoreProviders.add(new SingletonResourceProvider(new FileDownloader(srcDocumentRoot)));

            List<ResourceProvider> rAllProviders = new ArrayList<ResourceProvider>(rCoreProviders);
//            rAllProviders.add(new SingletonResourceProvider(new TikaWelcome(tika, rCoreProviders)));
            sf.setResourceProviders(rAllProviders);

            List<Object> providers = new ArrayList<Object>();
            providers.add(new JSONViewerWriter());
            sf.setProviders(providers);

            sf.setAddress("http://" + host + ":" + port + "/");
            BindingFactoryManager manager = sf.getBus().getExtension(
                    BindingFactoryManager.class);
            JAXRSBindingFactory factory = new JAXRSBindingFactory();
            factory.setBus(sf.getBus());
            manager.registerBindingFactory(JAXRSBindingFactory.JAXRS_BINDING_ID,
                    factory);
            sf.create();
            logger.info("Started");
        } catch (Exception ex) {
            logger.fatal("Can't start", ex);
            System.exit(-1);
        }
    }
}
