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
    private V8TupleBuffer toV8TupleBuffer;
    private V8TupleBuffer fromV8TupleBuffer;

    private Environment environment;
    private V8Function flushToV8;

    public Environment getEnvironment() {
        return environment;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, int pipeId, Fields groupingFields,
                              Fields argumentFields, Fields resultFields,
                              Map<String, JSType> incomingTypes, Map<String, JSType> outgoingTypes) {

        this.environment = environment;
        V8ScriptEngine eng = environment.getEngine();

        toV8TupleBuffer = V8TupleBuffer.newOutput(eng, groupingFields, argumentFields, incomingTypes);
        fromV8TupleBuffer = V8TupleBuffer.newInput(eng, resultFields, outgoingTypes);

        try {
            flushToV8 = (V8Function)environment.invokeMethod(v8PipeClass, "get_flush_routine",
                    toV8TupleBuffer.getBuffer(), fromV8TupleBuffer.getBuffer(), pipeId);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    public void setOutputEntryCollector(TupleEntryCollector out) {
        fromV8TupleBuffer.setOutCollector(out);
    }

    public void addArgument(TupleEntry argument) {
        this.toV8TupleBuffer.addArgument(argument);
    }

    public void addGroup(TupleEntry group) {
        this.toV8TupleBuffer.addGroup(group);
    }

    public boolean isFull() {
        return this.toV8TupleBuffer.isFullForArguments();
    }

    public void flushToV8(boolean finalizeGroup, boolean finalizeBuffer) {
        this.toV8TupleBuffer.fillV8Arrays();
        flushToV8.invoke(finalizeGroup, finalizeBuffer);
        this.toV8TupleBuffer.clear();
    }

    public void closeGroup() {
        this.toV8TupleBuffer.closeGroup();
    }

}
