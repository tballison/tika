package org.apache.tika.eval;

import java.util.Map;

public interface TableWriter {
    public enum Header {};

    public void writeRow(Map<Header, String> data);

    public void writeRow(Iterable<String> row);
}
