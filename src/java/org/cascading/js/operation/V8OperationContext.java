package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import java.util.Arrays;

public class V8OperationContext {
    private static int OUTGOING_BUFFER_SIZE = 1024 * 8;

    private Environment environment;
    private V8Object v8PipeClass;
    private Object[] buffer = null;
    private int bufferCount = 0;
    private Fields argumentFields = null;

    public Environment getEnvironment() {
        return environment;
    }

    public V8Object getV8PipeClass() {
        return v8PipeClass;
    }

    public boolean bufferIsFull() {
        return bufferCount == OUTGOING_BUFFER_SIZE * argumentFields.size();
    }

    public void clearBuffer() {
        bufferCount = 0;
    }

    public Object[] getBuffer() {
        if (bufferCount == OUTGOING_BUFFER_SIZE * argumentFields.size()) {
            return buffer;
        } else {
            return Arrays.copyOfRange(buffer, 0, bufferCount);
        }
    }

    public void addToBuffer(TupleEntry entry) {
        int numFields = this.argumentFields.size();

        for (int i = 0; i < numFields; i++) {
            buffer[bufferCount] = entry.get(argumentFields.getPos(argumentFields.get(i)));
            bufferCount++;
        }
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, Fields argumentFields) {
        this.environment = environment;
        this.v8PipeClass = v8PipeClass;
        this.argumentFields = argumentFields;
        this.buffer = new Object[argumentFields.size() * OUTGOING_BUFFER_SIZE];
    }
}
