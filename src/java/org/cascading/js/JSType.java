package org.cascading.js;

public enum JSType {
    INT(0),
    LONG(1),
    BOOL(2),
    DOUBLE(3),
    DATE(4),
    STRING(5);

    public final int idx;

    JSType(final int idx) {
        this.idx = idx;
    }
}
