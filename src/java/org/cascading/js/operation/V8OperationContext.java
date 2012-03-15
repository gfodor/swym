package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Function;
import lu.flier.script.V8Object;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.JSType;
import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.util.Map;

public class V8OperationContext {
    private V8TupleBuffer outTupleBuffer;
    private V8TupleBuffer inTupleBuffer;

    private Environment environment;
    private V8Function flushToV8;

    public Environment getEnvironment() {
        return environment;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, int pipeId, Fields groupingFields, Fields argumentFields, Fields resultFields, Map<String, JSType> typeMap) {
        this.environment = environment;
        V8ScriptEngine eng = environment.getEngine();

        outTupleBuffer = V8TupleBuffer.newOutput(eng, groupingFields, argumentFields, typeMap);
        inTupleBuffer = V8TupleBuffer.newInput(eng, resultFields, typeMap);

        try {
            flushToV8 = (V8Function)environment.invokeMethod(v8PipeClass, "get_flush_routine",
                    outTupleBuffer.getBuffer(), inTupleBuffer.getBuffer(), pipeId);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    public void setOutputEntryCollector(TupleEntryCollector out) {
        inTupleBuffer.setOutCollector(out);
    }

    public void addArgument(TupleEntry argument) {
        this.outTupleBuffer.addArgument(argument);
    }

    public void addGroup(TupleEntry group) {
        this.outTupleBuffer.addGroup(group);
    }

    public boolean isFull() {
        return this.outTupleBuffer.isFullForArguments();
    }

    public void flushToV8(boolean finalizeGroup, boolean finalizeBuffer) {
        this.outTupleBuffer.fillV8Arrays();
        flushToV8.invoke(finalizeGroup, finalizeBuffer);
        this.outTupleBuffer.clear();
    }

    public void closeGroup() {
        this.outTupleBuffer.closeGroup();
    }

}
