package org.apache.tika.batch.builders;


import org.w3c.dom.Node;

import java.util.Map;

/**
 * Interface for things that build objects from a DOM Node and a map of runtime attributes
 * @param <T>
 */
public interface ObjectFromDOMBuilder<T> {

    public T build(Node node, Map<String, String> runtimeAttributes);
}
