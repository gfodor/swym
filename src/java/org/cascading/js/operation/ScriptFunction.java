package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public class ScriptFunction extends BaseOperation<V8OperationContext> implements Function<V8OperationContext> {
    private Environment.EnvironmentArgs environmentArgs;
    private int pipeIndex;

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

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> operationCall) {
        operationCall.getContext().getEnvironment().shutdown();
    }

    public ScriptFunction(Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeIndex) {
        super(resultFields.size(), resultFields);
        this.environmentArgs = environmentArgs;
        this.pipeIndex = pipeIndex;
    }

    private class Emitter {
        FunctionCall<V8OperationContext> call;
        ScriptFunction function;

        public Emitter(ScriptFunction function, FunctionCall<V8OperationContext> call) {
            this.call = call;
            this.function = function;
        }

        public void emit(V8Object out) {
            Object[] outVals = new Comparable[function.fieldDeclaration.size()];

            for (int i = 0; i < function.fieldDeclaration.size(); i++) {
                outVals[i] = out.get(function.fieldDeclaration.get(i));
            }

            call.getOutputCollector().add(new TupleEntry(new Tuple(outVals)));
        }
    }

    public void operate(FlowProcess flowProcess, FunctionCall<V8OperationContext> call) {
        TupleEntry entry = call.getArguments();
        V8OperationContext ctx = call.getContext();
        Environment env = ctx.getEnvironment();
        Fields fields = entry.getFields();

        try {
            V8Object in = (V8Object)env.evaluateScript("{}");

            for (int i = 0; i < fields.size(); i++) {
                in.put(fields.get(i).toString(), entry.get(i));
            }

            env.invokeMethod(ctx.getV8PipeClass(), "invokePipeCallback",
                    pipeIndex, "default", in, new Emitter(this, call));
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}

