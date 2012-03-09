package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.cascading.js.util.Environment;

public class ScriptFunction extends ScriptOperation implements Function<V8OperationContext> {
    public ScriptFunction(Fields argumentSelector, Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeId) {
        super(argumentSelector, resultFields, environmentArgs, pipeId);
    }

    public void operate(FlowProcess flowProcess, FunctionCall<V8OperationContext> call) {
        TupleEntry entry = call.getArguments();
        V8OperationContext ctx = call.getContext();

        ctx.addArgument(entry);

        if (ctx.isFull()) {
            flushToV8(call);
        }
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> operationCall) {
        flushToV8(operationCall);
        operationCall.getContext().getEnvironment().shutdown();
    }
}

