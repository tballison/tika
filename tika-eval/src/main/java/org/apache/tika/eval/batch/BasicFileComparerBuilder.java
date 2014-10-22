package org.apache.tika.eval.batch;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.tika.batch.ConsumersManager;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.batch.builders.AbstractConsumersBuilder;
import org.apache.tika.batch.builders.BatchProcessBuilder;
import org.apache.tika.eval.BasicFileComparer;
import org.apache.tika.util.XMLDOMUtil;
import org.w3c.dom.Node;

public class BasicFileComparerBuilder extends AbstractConsumersBuilder {
    @Override
    public ConsumersManager build(Node node, Map<String, String> runtimeAttributes, ArrayBlockingQueue<FileResource> queue) {
        List<FileResourceConsumer> consumers = new LinkedList<FileResourceConsumer>();
        int numConsumers = BatchProcessBuilder.getNumConsumers(runtimeAttributes);

        Map<String, String> localAttrs = XMLDOMUtil.mapifyAttrs(node, runtimeAttributes);
        File thisRootDir = getFile(localAttrs, "thisDir");
        File thatRootDir = getFile(localAttrs, "thatDir");
        File outputFile = getFile(localAttrs, "outputFile");
        File langModelDir = getFile(localAttrs, "langModelDir");
        BasicFileComparer.setLangModelDir(langModelDir);

        for (int i = 0; i < numConsumers; i++) {
            FileResourceConsumer consumer = new BasicFileComparer(queue, thisRootDir, thatRootDir);
            consumers.add(consumer);
        }
        return new BasicFileComparerManager(consumers, outputFile);
    }

    private File getFile(Map<String, String> attrs, String key) {
        String filePath = attrs.get(key);
        if (filePath == null) {
            throw new RuntimeException("must specify outputFile");
        }
        return new File(filePath);
    }
}
