package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Object;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.util.Environment;

public class V8OperationContext {
    private V8TupleBuffer tupleBuffer;
    private Environment environment;
    private TupleEntryCollector outputEntryCollector;

    public Environment getEnvironment() {
        return environment;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, int pipeId, Fields groupingFields, Fields argumentFields, Fields resultFields) {
        this.environment = environment;
        V8ScriptEngine eng = environment.getEngine();

        tupleBuffer = new V8TupleBuffer(eng, groupingFields, argumentFields, null);

        /*V8Function emitCallback = eng.createFunction(this, "emit");

        argumentProcessor = (V8Function)environment.invokeMethod(v8PipeClass, "get_argument_processor",
                this.tupleTransfer.getGroupTuple(), this.tupleTransfer.getArgumentTuple(), emitCallback, pipeId);

        eng.compile("var foo = function() { }").eval();
        Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
        argumentProcessor = (V8Function)scope.get("foo");*/
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
        // TODO fire routine
        this.tupleBuffer.clear();
    }

    public void closeGroup() {
        this.tupleBuffer.closeGroup();
    }

}
