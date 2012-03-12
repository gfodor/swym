package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Function;
import lu.flier.script.V8Object;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.util.Map;

public class V8OperationContext {
    private V8TupleBuffer tupleBuffer;
    private Environment environment;
    private TupleEntryCollector outputEntryCollector;
    private V8Function flushToV8;

    public Environment getEnvironment() {
        return environment;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, int pipeId, Fields groupingFields, Fields argumentFields, Fields resultFields, Map<String, V8TupleBuffer.JSType> typeMap) {
        this.environment = environment;
        V8ScriptEngine eng = environment.getEngine();

        tupleBuffer = new V8TupleBuffer(eng, groupingFields, argumentFields, typeMap);

        try {
            flushToV8 = (V8Function)environment.invokeMethod(v8PipeClass, "get_flush_routine",
                    tupleBuffer.getBuffer(), eng.createFunction(this, "flushFromV8"), pipeId);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    public void setOutputEntryCollector(TupleEntryCollector out) {
        if (this.outputEntryCollector != out) {
            this.outputEntryCollector = out;
        }
    }

    public void addArgument(TupleEntry argument) {
        this.tupleBuffer.addArgument(argument);
    }

    public void addGroup(TupleEntry group) {
        this.tupleBuffer.addGroup(group);
    }

    public boolean isFull() {
        return this.tupleBuffer.isFull();
    }

    public void flush() {
        this.tupleBuffer.fillV8Arrays();
        flushToV8.invokeVoid();
        this.tupleBuffer.clear();
    }

    public void flushFromV8() {
        System.out.println("Flushing from V8");
    }

    public void closeGroup() {
        this.tupleBuffer.closeGroup();
    }

}
