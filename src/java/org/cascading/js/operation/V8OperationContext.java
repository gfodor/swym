package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Array;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

public class V8OperationContext {
    private static int OUTGOING_BUFFER_SIZE = 1024 * 8;

    private V8TupleBuffer tupleBuffer;
    private Environment environment;
    private V8Object v8PipeClass;

    public Environment getEnvironment() {
        return environment;
    }

    public V8Object getV8PipeClass() {
        return v8PipeClass;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, Fields groupingFields, Fields argumentFields) {
        this.environment = environment;
        this.v8PipeClass = v8PipeClass;
        this.tupleBuffer = new V8TupleBuffer(environment.getEngine(), groupingFields, argumentFields);
    }

    public void addGroup(TupleEntry group) {
        tupleBuffer.addGroup(group);
    }

    public void addArgument(TupleEntry arg) {
        tupleBuffer.addArgument(arg);
    }

    public void closeGroup() {
        tupleBuffer.closeGroup();
    }

    public void clear() {
        tupleBuffer.clear();
    }

    public boolean isFull() {
        return tupleBuffer.isFull();
    }

    public V8Array getPackage() {
        return tupleBuffer.getPackage(this.environment.getEngine());
    }

    public int getGroupCount() {
        return tupleBuffer.getGroupCount();
    }

    public int getTupleCount() {
        return tupleBuffer.getTupleCount();
    }
}
