package org.apache.tika.eval.batch;

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
