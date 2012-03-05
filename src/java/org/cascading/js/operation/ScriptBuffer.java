package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import java.util.Iterator;

public class ScriptBuffer extends ScriptOperation implements Buffer<V8OperationContext> {
    private Fields groupingFields;
    private Fields bufferFields;
    private V8Object delimiter;
    private V8Object terminator;

    public static String STUB_FIELD_PREFIX = "___swym_stub_gk_";

    public ScriptBuffer(Fields groupingFields, Fields argumentSelector, Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeId) {
        super(argumentSelector.size() + groupingFields.size(), argumentSelector, resultFields, environmentArgs, pipeId);
        this.groupingFields = groupingFields;
        this.bufferFields = argumentSelector.subtract(groupingFields);
    }

    public void operate(FlowProcess flowProcess, BufferCall<V8OperationContext> call) {
        V8OperationContext ctx = call.getContext();

        if (delimiter == null) {
            delimiter = ctx.getEnvironment().createObject();
            terminator = ctx.getEnvironment().createObject();
        }

        ctx.addObjectToBuffer(delimiter);

        Iterator<TupleEntry> arguments = call.getArgumentsIterator();
        ctx.addToBuffer(call.getGroup(), groupingFields);

        while (arguments.hasNext()) {
            ctx.addToBuffer(arguments.next(), bufferFields);

            if (ctx.bufferIsFull()) {
                flushToV8(call, delimiter, terminator);
                ctx.addObjectToBuffer(delimiter);
                ctx.addToBuffer(call.getGroup(), groupingFields);
            }
        }

        ctx.addObjectToBuffer(terminator);
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> call) {
        call.getContext().addObjectToBuffer(terminator);
        flushToV8(call, delimiter, terminator);
        call.getContext().getEnvironment().shutdown();
    }
}
