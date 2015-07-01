package org.apache.tika.eval.db;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableInfo {

    private final String name;
    private final List<ColInfo> colInfos = new ArrayList<ColInfo>();
    private final Set<Cols> colNames = new HashSet<Cols>();

    public TableInfo(String name, ColInfo... cols) {
        Collections.addAll(colInfos, cols);
        Collections.unmodifiableList(colInfos);
        this.name = name;
        for (ColInfo c : colInfos) {
            assert (!colNames.contains(c.getName()));
            colNames.add(c.getName());
        }
    }

    public TableInfo(String name, List<ColInfo> cols) {
        colInfos.addAll(cols);
        Collections.unmodifiableList(colInfos);
        this.name = name;
        for (ColInfo c : colInfos) {
            assert (!colNames.contains(c.getName()));
            colNames.add(c.getName());
        }
    }

    public String getName() {
        return name;
    }

    public List<ColInfo> getColInfos() {
        return colInfos;
    }

    public boolean containsColumn(Cols cols) {
        return colNames.contains(cols);
    }
}

