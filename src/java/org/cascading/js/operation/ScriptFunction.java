package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.cascading.js.JSType;
import org.cascading.js.util.Environment;

import java.util.Map;

public class ScriptFunction extends ScriptOperation implements Function<V8OperationContext> {
    public ScriptFunction(Fields argumentSelector, Map<String, JSType> argumentTypes,
                          Fields resultFields, Map<String, JSType> resultTypes,
                          Environment.EnvironmentArgs environmentArgs, int pipeId) {
        super(argumentSelector, argumentTypes, resultFields, resultTypes, environmentArgs, pipeId);
    }

    public void operate(FlowProcess flowProcess, FunctionCall<V8OperationContext> call) {
        TupleEntry entry = call.getArguments();
        V8OperationContext ctx = call.getContext();

        ctx.addArgument(entry);
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> operationCall) {
        operationCall.getContext().getEnvironment().shutdown();
    }
}

