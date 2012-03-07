package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Array;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public abstract class ScriptOperation extends BaseOperation<V8OperationContext> {
    protected Environment.EnvironmentArgs environmentArgs;
    protected int pipeId;
    protected Fields argumentSelector;

    private boolean isFunction = false;
    private boolean isBuffer = false;
    private int bufferTupleWidth;

    public ScriptOperation(int bufferTupleWidth, Fields argumentSelector, Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeId) {
        super(resultFields.size(), resultFields);

        this.bufferTupleWidth = bufferTupleWidth;
        this.argumentSelector = argumentSelector;
        this.environmentArgs = environmentArgs;
        this.pipeId = pipeId;

        if (this instanceof Buffer) {
            isBuffer = true;
        }

        if (this instanceof Function) {
            isFunction = true;
        }
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

            env.evaluateScript("var __v8PipeClass;");
            env.evaluateScript("require(['" + pathPrefix + "cascading/components'], " +
                              "function(components) { __v8PipeClass = components.Pipe; });");

            V8Object v8PipeClass = (V8Object)env.extractObject("__v8PipeClass");
            env.evaluateScript("delete __v8PipeClass");

            operationCall.setContext(new V8OperationContext(env, v8PipeClass, bufferTupleWidth));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }

    V8Array bufferArray = null;

    protected void flushToV8(OperationCall<V8OperationContext> call) {
        this.flushToV8(call, null, null);
    }

    protected void flushToV8(OperationCall<V8OperationContext> call, V8Object delimiter, V8Object terminator) {
        V8OperationContext ctx = call.getContext();
        Environment env = ctx.getEnvironment();

        try {
            if (bufferArray == null) {
                bufferArray = env.createArray(ctx.getBuffer());
            } else {
                bufferArray.setElements(ctx.getBuffer());
            }

            env.invokeMethod(ctx.getV8PipeClass(), "process_tuples",
                    pipeId, bufferArray, ctx.bufferCount(), this, call, delimiter, terminator);

            ctx.clearBuffer();
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public void flushFromV8(V8Array array, int size, OperationCall<V8OperationContext> call) {
        TupleEntryCollector collector = null;

        if (isFunction) {
            collector = ((FunctionCall<V8OperationContext>)call).getOutputCollector();
        } else if (isBuffer) {
            collector = ((BufferCall<V8OperationContext>)call).getOutputCollector();
        } else {
            throw new RuntimeException("Unsupported ScriptOperation. Not a buffer or a function.");
        }

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
}