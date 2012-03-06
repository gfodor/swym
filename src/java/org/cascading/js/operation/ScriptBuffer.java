package org.cascading.js.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import java.util.Iterator;

public class ScriptBuffer extends ScriptOperation implements Buffer<V8OperationContext> {
    private Fields bufferFields;
    private V8Object delimiter;
    private V8Object terminator;

    public static String STUB_FIELD_PREFIX = "___swym_stub_gk_";

    public ScriptBuffer(Fields groupingFields, Fields argumentSelector, Fields resultFields, Environment.EnvironmentArgs environmentArgs, int pipeId) {
        super(argumentSelector.size() + groupingFields.size(), argumentSelector, resultFields, environmentArgs, pipeId);
        this.bufferFields = argumentSelector.subtract(groupingFields);

        groupFieldOffsets = new int[groupingFields.size()];

        for (int i = 0; i < groupFieldOffsets.length; i++) {
            groupFieldOffsets[i] = groupingFields.getPos(groupingFields.get(i));
        }

        argumentFieldOffsets = new int[bufferFields.size()];

        for (int i = 0; i < argumentFieldOffsets.length; i++) {
            argumentFieldOffsets[i] = bufferFields.getPos(bufferFields.get(i));
        }
    }

    private final int[] groupFieldOffsets;
    private final int[] argumentFieldOffsets;

    public void operate(FlowProcess flowProcess, BufferCall<V8OperationContext> call) {
        V8OperationContext ctx = call.getContext();

        if (delimiter == null) {
            delimiter = ctx.getEnvironment().createObject();
            terminator = ctx.getEnvironment().createObject();
        }

        ctx.addObjectToBuffer(delimiter);

        Iterator<TupleEntry> arguments = call.getArgumentsIterator();
        TupleEntry group = call.getGroup();

        for (int groupFieldOffset : groupFieldOffsets) {
            ctx.addObjectToBuffer(group.get(groupFieldOffset));
        }

        while (arguments.hasNext()) {
            TupleEntry next = arguments.next();

            for (int argumentFieldOffset : argumentFieldOffsets) {
                ctx.addObjectToBuffer(next.get(argumentFieldOffset));
            }

            if (ctx.bufferIsFull()) {
                flushToV8(call, delimiter, terminator);
                ctx.addObjectToBuffer(delimiter);

                for (int groupFieldOffset : groupFieldOffsets) {
                    ctx.addObjectToBuffer(group.get(groupFieldOffset));
                }
            }
        }

        ctx.addObjectToBuffer(terminator);
    }

    private long t0;

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<V8OperationContext> operationCall ) {
        super.prepare(flowProcess, operationCall);
        t0 = System.currentTimeMillis();
    }

    public void cleanup(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall<V8OperationContext> call) {
        call.getContext().addObjectToBuffer(terminator);
        flushToV8(call, delimiter, terminator);
        System.out.println("Buffer time: " + (System.currentTimeMillis() - t0));
        call.getContext().getEnvironment().shutdown();
    }
}
