package org.apache.tika.batch.builders;


import org.apache.tika.batch.FileResource;
import org.w3c.dom.Node;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Same as {@link org.apache.tika.batch.builders.ObjectFromDOMAndQueueBuilder},
 * but this is for objects that require access to the common queue.
 * @param <T>
 */
public interface ObjectFromDOMAndQueueBuilder<T> {

    public T build(Node node, Map<String, String> runtimeAttributes,
                   ArrayBlockingQueue<FileResource> resourceQueue);

}
