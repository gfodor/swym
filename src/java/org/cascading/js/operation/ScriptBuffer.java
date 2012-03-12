package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.cascading.js.util.Environment;

import java.util.Iterator;

public class ScriptBuffer extends ScriptOperation implements Buffer<V8OperationContext> {
    private Fields groupingFields;

    public ScriptBuffer(Fields groupingFields, Fields argumentSelector, Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeId) {
        super(argumentSelector, resultFields, environmentArgs, pipeId);
        this.groupingFields = groupingFields;
    }

    @Override
    protected Fields getGroupingFields() {
        return groupingFields;
    }

    long timeInInvoke = 0;

    long timeSetGroup = 0;
    long timeSetArg = 0;

    public static String STUB_FIELD_PREFIX = "___swym_stub_gk_";

    public void operate(FlowProcess flowProcess, BufferCall<V8OperationContext> call) {
        V8OperationContext ctx = call.getContext();

        Iterator<TupleEntry> arguments = call.getArgumentsIterator();
        TupleEntry group = call.getGroup();
        ctx.addGroup(group);
        ctx.setOutputEntryCollector(call.getOutputCollector());

        long t0 = System.currentTimeMillis();
        timeSetGroup += (System.currentTimeMillis() - t0);

        while (arguments.hasNext()) {
            TupleEntry next = arguments.next();
            t0 = System.currentTimeMillis();
            ctx.addArgument(next);

            if (ctx.isFull()) {
                ctx.flush();
            }

            timeSetArg += (System.currentTimeMillis() - t0);
        }
    }

    private long t0;

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<V8OperationContext> operationCall ) {
        super.prepare(flowProcess, operationCall);
        t0 = System.currentTimeMillis();
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> call) {
        super.cleanup(flowProcess, call);
        call.getContext().closeGroup();
        call.getContext().flush();
        System.out.println("Buffer time: " + (System.currentTimeMillis() - t0) + " " + timeInInvoke + " " + timeSetArg + " " + timeSetGroup);
        call.getContext().getEnvironment().shutdown();
    }
}
