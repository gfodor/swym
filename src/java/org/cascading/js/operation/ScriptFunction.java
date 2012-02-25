package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.cascading.js.util.Environment;
import org.mozilla.javascript.NativeFunction;
import org.mozilla.javascript.Scriptable;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ScriptFunction extends BaseOperation<ScriptOperationContext> implements Function<ScriptOperationContext> {
    private Environment.EnvironmentArgs environmentArgs;
    private int pipeIndex;
    private long convertTime = 0;
    private long invokeTime = 0;
    private long emitTime = 0;
    private long createCount = 0;

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<ScriptOperationContext> operationCall ) {
        Environment env = new Environment();
        try {
            env.start(environmentArgs);

            String pathPrefix = "";

            if (environmentArgs.isRunTests()) {
                pathPrefix = "../../../../../";
            }

            env.evaluateScript("var __dummy;");
            env.evaluateScript("require([\"" + pathPrefix + "src/js/org/cascading/js/components\"], function(components) {" +
                               "  __dummy = components.Pipe; });");
            NativeFunction pipeClass = (NativeFunction)env.extractObject("__dummy");
            env.evaluateScript("delete __dummy");

            operationCall.setContext(new ScriptOperationContext(env, pipeClass));
            env.declareScriptable(TempTuple.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<ScriptOperationContext> call) {
        ScriptOperationContext ctx = call.getContext();
        Environment env = ctx.getEnvironment();
        System.err.println("Convert time : " + convertTime);
        System.err.println("Invoke time : " + invokeTime);
        System.err.println("Emit time : " + emitTime);
        System.err.println("Create count: " + createCount);
        call.getContext().getEnvironment().shutdown();
    }

    public ScriptFunction(Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeIndex) {
        super(resultFields.size(), resultFields);
        this.environmentArgs = environmentArgs;
        this.pipeIndex = pipeIndex;
    }

    public void flush(String out, FunctionCall<ScriptOperationContext> call) {
        long t0 = System.currentTimeMillis();

        if (out != null) {
            Object[] outVals = new Comparable[fieldDeclaration.size()];

            for (int j = 0; j < fieldDeclaration.size(); j++) {
                outVals[j] = out;
            }

            call.getOutputCollector().add(new TupleEntry(new Tuple(outVals)));
        }

        emitTime += System.currentTimeMillis() - t0;
    }

    public void operate(FlowProcess flowProcess, FunctionCall<ScriptOperationContext> call) {
        TupleEntry entry = call.getArguments();
        ScriptOperationContext ctx = call.getContext();
        Environment env = ctx.getEnvironment();

        try {
            long t0 = System.currentTimeMillis();
            Scriptable tempTuple = env.createScriptable("TempTuple", new Object[] { });
            ((TempTuple)tempTuple).setEntry(entry);
            convertTime += System.currentTimeMillis() - t0;
            t0 = System.currentTimeMillis();

            env.invokeMethod(ctx.getPipeClass(), "invokePipeCallback",
                    pipeIndex, "default", tempTuple, this, call);

            invokeTime += System.currentTimeMillis() - t0;
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

