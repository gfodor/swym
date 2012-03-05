package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

public class V8OperationContext {
    private static int OUTGOING_BUFFER_SIZE = 1024 * 8;

    private Environment environment;
    private V8Object v8PipeClass;
    private Object[] buffer = null;
    private int bufferCount = 0;
    private int tupleWidth;

    public Environment getEnvironment() {
        return environment;
    }

    public V8Object getV8PipeClass() {
        return v8PipeClass;
    }

    public int maxBufferSize() {
        return OUTGOING_BUFFER_SIZE * tupleWidth;
    }

    public boolean bufferIsFull() {
        return bufferCount >= maxBufferSize();
    }

    public void clearBuffer() {
        bufferCount = 0;
    }

    public int bufferCount() {
       return bufferCount;
    }

    public Object[] getBuffer() {
        return buffer;
    }

    public void addToBuffer(TupleEntry entry, Fields fields) {
        int numFields = fields.size();

        for (int i = 0; i < numFields; i++) {
            buffer[bufferCount] = entry.get(fields.getPos(fields.get(i)));
            bufferCount++;
        }
    }

    public void addObjectToBuffer(Object object) {
        buffer[bufferCount] = object;
        bufferCount++;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, int tupleWidth) {
        this.environment = environment;
        this.v8PipeClass = v8PipeClass;
        this.tupleWidth = tupleWidth;
        this.buffer = new Object[maxBufferSize() + 1024];
    }
}
