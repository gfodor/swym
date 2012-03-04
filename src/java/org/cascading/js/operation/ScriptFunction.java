package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Array;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public class ScriptFunction extends BaseOperation<V8OperationContext> implements Function<V8OperationContext> {
    private Environment.EnvironmentArgs environmentArgs;
    private int pipeId;
    private Fields argumentSelector;

    public ScriptFunction(Fields argumentSelector, Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeId) {
        super(resultFields.size(), resultFields);
        this.argumentSelector = argumentSelector;
        this.environmentArgs = environmentArgs;
        this.pipeId = pipeId;
    }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<V8OperationContext> operationCall ) {
        Environment env = new Environment();
        try {
            env.start(environmentArgs);

            String pathPrefix = "";

            if (environmentArgs.isLoadTestFramework()) {
                pathPrefix = "../../lib/js";
            }

            env.evaluateScript("var __dummy;");
            env.evaluateScript("require([\"" + pathPrefix + "cascading/components\"], function(components) {" +
                               "  __dummy = components.Pipe; });");
            V8Object v8PipeClass = (V8Object)env.extractObject("__dummy");
            env.evaluateScript("delete __dummy");

            operationCall.setContext(new V8OperationContext(env, v8PipeClass, argumentSelector));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }

    V8Array bufferArray = null;

    private void flushToV8(OperationCall<V8OperationContext> call) {
        V8OperationContext ctx = call.getContext();
        Environment env = ctx.getEnvironment();

        try {
            if (bufferArray == null) {
                bufferArray = env.createArray(ctx.getBuffer());
            } else {
                bufferArray.setElements(ctx.getBuffer());
            }

            env.invokeMethod(ctx.getV8PipeClass(), "process_tuples",
                    pipeId, bufferArray, ctx.bufferCount(), this, call);

            ctx.clearBuffer();
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public void flushFromV8(V8Array array, int size, FunctionCall<V8OperationContext> call) {
        TupleEntryCollector collector = call.getOutputCollector();
        Fields outFields = this.getFieldDeclaration();
        int numOutFields = outFields.size();

        if (collector != null) {
            Object[] output = array.toArray();
            Object[] tuple = null;

            for (int i = 0 ; i < size; i++) {
                if (i % numOutFields == 0) {
                    tuple = new Object[numOutFields];
                }

                tuple[i % numOutFields] = output[i];

                if (i % numOutFields == numOutFields - 1) {
                    collector.add(new Tuple(tuple));
                }
            }
        }
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> operationCall) {
        flushToV8(operationCall);
        operationCall.getContext().getEnvironment().shutdown();
    }

    public void operate(FlowProcess flowProcess, FunctionCall<V8OperationContext> call) {
        TupleEntry entry = call.getArguments();
        V8OperationContext ctx = call.getContext();

        ctx.addToBuffer(entry);

        if (ctx.bufferIsFull()) {
            flushToV8(call);
        }
    }
}

