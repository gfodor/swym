package org.cascading.js.operation;

import cascading.tuple.TupleEntry;
import org.mozilla.javascript.ScriptableObject;

public class TempTuple extends ScriptableObject {
    private static final long serialVersionUID = 433270532227335642L;
    private TupleEntry entry;

    public TempTuple() { }

    @Override
    public String getClassName() {
        return "TempTuple";
    }

    public void jsConstructor() {
    }

    public void setEntry(TupleEntry entry) {
        this.entry = entry;
    }

    public String jsGet_line() {
        return (String)entry.get(0);
    }
}


