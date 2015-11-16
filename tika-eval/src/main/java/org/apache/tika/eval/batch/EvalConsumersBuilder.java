package org.apache.tika.eval.batch;


import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.tika.batch.ConsumersManager;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.batch.builders.AbstractConsumersBuilder;
import org.apache.tika.batch.builders.BatchProcessBuilder;
import org.apache.tika.eval.AbstractProfiler;
import org.apache.tika.eval.LanguageIDWrapper;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.eval.db.H2Util;
import org.apache.tika.util.ClassLoaderUtil;
import org.apache.tika.util.PropsUtil;
import org.apache.tika.util.XMLDOMUtil;
import org.w3c.dom.Node;

public class EvalConsumersBuilder extends AbstractConsumersBuilder {

    private final static String WHICH_DB = "h2";//TODO: allow parameterization

    @Override
    public ConsumersManager build(Node node, Map<String, String> runtimeAttributes,
                                  ArrayBlockingQueue<FileResource> queue) {

        List<FileResourceConsumer> consumers = new LinkedList<>();
        int numConsumers = BatchProcessBuilder.getNumConsumers(runtimeAttributes);

        Map<String, String> localAttrs = XMLDOMUtil.mapifyAttrs(node, runtimeAttributes);

        Path dbDir = getPath(localAttrs, "dbDir");
        Path langModelDir = getPath(localAttrs, "langModelDir");

        try {
            if (langModelDir == null) {
                LanguageIDWrapper.loadBuiltInModels();
            } else {
                LanguageIDWrapper.loadModels(langModelDir);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path commonWords = getNonNullPath(localAttrs, "commonWords");
        try {
            AbstractProfiler.loadCommonWords(commonWords);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        boolean append = PropsUtil.getBoolean(localAttrs.get("dbAppend"), false);

        //parameterize which db util to use
        DBUtil util = new H2Util(dbDir);
        EvalConsumerBuilder consumerBuilder = ClassLoaderUtil.buildClass(EvalConsumerBuilder.class,
                PropsUtil.getString(localAttrs.get("consumerBuilderClass"), null));
        if (consumerBuilder == null) {
            throw new RuntimeException("Must specify consumerBuilderClass in config file");
        }
        consumerBuilder.init(queue, localAttrs, util);

        try {
            util.createDB(consumerBuilder.getTableInfo(), append);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < numConsumers; i++) {
            try {
                consumers.add(consumerBuilder.build());
            } catch (IOException | SQLException e) {
                throw new RuntimeException(e);
            }
        }

        DBConsumersManager manager;
        try {
            manager = new DBConsumersManager(util, consumers);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        consumerBuilder.addErrorLogTablePairs(manager);

        return manager;
    }

    private Path getNonNullPath(Map<String, String> attrs, String key) {
        Path p = getPath(attrs, key);
        if (p == null) {
            throw new RuntimeException("Must specify a file for this attribute: "+key);
        }
        return p;
    }


    protected Path getPath(Map<String, String> attrs, String key) {
        String filePath = attrs.get(key);
        if (filePath == null) {
            return null;
        }
        return Paths.get(filePath);
    }


}
