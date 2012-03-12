package org.cascading.js;

public enum FieldSet {
    ARGS(0),
    GROUP(1),
    RESULT(2);

    public final int idx;

    FieldSet(final int idx) {
        this.idx = idx;
    }
}
