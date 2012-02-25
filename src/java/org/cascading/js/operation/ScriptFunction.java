package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Array;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public class ScriptFunction extends BaseOperation<V8OperationContext> implements Function<V8OperationContext> {
    private Environment.EnvironmentArgs environmentArgs;
    private int pipeIndex;
    private long convertTime = 0;
    private long invokeTime = 0;
    private long emitTime = 0;
    private long createCount = 0;

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<V8OperationContext> operationCall ) {
        Environment env = new Environment();
        try {
            env.start(environmentArgs);

            String pathPrefix = "";

            if (environmentArgs.isLoadTestFramework()) {
                pathPrefix = "../../../../../";
            }

            env.evaluateScript("var __dummy;");
            env.evaluateScript("require([\"" + pathPrefix + "src/js/org/cascading/js/components\"], function(components) {" +
                               "  __dummy = components.Pipe; });");
            V8Object v8PipeClass = (V8Object)env.extractObject("__dummy");
            env.evaluateScript("delete __dummy");

            operationCall.setContext(new V8OperationContext(env, v8PipeClass));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> call) {
        V8OperationContext ctx = call.getContext();
        Environment env = ctx.getEnvironment();
        System.err.println("Convert time : " + convertTime);
        System.err.println("Invoke time : " + invokeTime);
        System.err.println("Emit time : " + emitTime);
        System.err.println("Create count: " + createCount);
        try {
            env.invokeMethod(ctx.getV8PipeClass(), "invokePipeCallback",
                    pipeIndex, "cleanup", this, call);
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        call.getContext().getEnvironment().shutdown();
    }

    public ScriptFunction(Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeIndex) {
        super(resultFields.size(), resultFields);
        this.environmentArgs = environmentArgs;
        this.pipeIndex = pipeIndex;
    }

    public void flush(V8Array buffer, FunctionCall<V8OperationContext> call) {
        for (int i = 0; i < buffer.size(); i++) {
            String out = (String)buffer.get(i);

            if (out != null) {
                Object[] outVals = new Comparable[fieldDeclaration.size()];

                long t0 = System.currentTimeMillis();

                for (int j = 0; j < fieldDeclaration.size(); j++) {
                    outVals[j] = out;
                }

                emitTime += System.currentTimeMillis() - t0;

                call.getOutputCollector().add(new TupleEntry(new Tuple(outVals)));
            }
        }
    }

    public void operate(FlowProcess flowProcess, FunctionCall<V8OperationContext> call) {
        TupleEntry entry = call.getArguments();
        V8OperationContext ctx = call.getContext();
        Environment env = ctx.getEnvironment();
        Fields fields = entry.getFields();

        try {
            long t0 = System.currentTimeMillis();
            V8Object in = env.createObject();

            for (int i = 0; i < fields.size(); i++) {
                in.put(fields.get(i).toString(), entry.get(i));
            }

            convertTime += System.currentTimeMillis() - t0;
            t0 = System.currentTimeMillis();

            env.invokeMethod(ctx.getV8PipeClass(), "invokePipeCallback",
                    pipeIndex, "default", in, this, call);

            invokeTime += System.currentTimeMillis() - t0;
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}

