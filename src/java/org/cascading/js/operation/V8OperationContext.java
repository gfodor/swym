package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Array;
import lu.flier.script.V8Function;
import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.util.Date;

public class V8OperationContext {
    private static int OUTGOING_BUFFER_SIZE = 1024 * 8;

    private V8TupleTransfer tupleTransfer;
    private Environment environment;
    private V8Function argumentProcessor;
    private V8Function groupStartProcessor;
    private V8Function groupEndProcessor;

    private V8Array outInt;
    private int[] jOutInt = new int[0];
    private V8Array outLong;
    private long[] jOutLong = new long[0];
    private V8Array outBool;
    private boolean[] jOutBool = new boolean[0];
    private V8Array outDouble;
    private double[] jOutDouble = new double[0];
    private V8Array outDate;
    private Date[] jOutDate = new Date[0];
    private V8Array outString;
    private String[] jOutString = new String[0];
    private V8Array outNullMap;
    private boolean[] jOutNullMap = new boolean[0];
    private V8Array outFieldTypes;
    private int[] jOutFieldTypes = new int[0];
    private V8Array outNumFieldsPerType;
    private int[] jOutNumFieldsPerType = new int[V8TupleTransfer.Type.values().length - 1];
    private V8Array outFieldDataOffsets;
    private int[] jOutFieldDataOffsets;
    private V8Array outArgs;
    private int[] jOutArgs = new int[1];

    private Fields resultFields;

    public Environment getEnvironment() {
        return environment;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, int pipeId, Fields groupingFields, Fields argumentFields, Fields resultFields) {
        this.environment = environment;
        this.resultFields = resultFields;
        this.tupleTransfer = new V8TupleTransfer(environment.getEngine(), groupingFields, argumentFields);
        this.outInt = environment.getEngine().createArray(new Object[0]);
        this.outLong = environment.getEngine().createArray(new Object[0]);
        this.outBool = environment.getEngine().createArray(new Object[0]);
        this.outDouble = environment.getEngine().createArray(new Object[0]);
        this.outDate = environment.getEngine().createArray(new Object[0]);
        this.outString = environment.getEngine().createArray(new Object[0]);
        this.outFieldTypes = environment.getEngine().createArray(new Object[0]);
        this.outFieldDataOffsets = environment.getEngine().createArray(new Object[resultFields.size()]);
        this.outNumFieldsPerType = environment.getEngine().createArray(jOutNumFieldsPerType);
        this.outNullMap = environment.getEngine().createArray(new Object[0]);

        // Out args: [number_of_tuples]
        this.outArgs = environment.getEngine().createArray(jOutArgs);
        jOutFieldTypes = new int[resultFields.size()];
        jOutFieldDataOffsets = new int[resultFields.size()];

        try {
            environment.invokeMethod(v8PipeClass, "set_pipe_out_buffers",
              outInt, outLong, outBool, outDouble, outDate, outString,
              outFieldTypes, outFieldDataOffsets, outNumFieldsPerType, outNullMap, outArgs, pipeId);

            argumentProcessor = (V8Function)environment.invokeMethod(v8PipeClass, "get_argument_processor",
                    this.tupleTransfer.getGroupTuple(), this.tupleTransfer.getArgumentTuple(), pipeId);

            groupStartProcessor = (V8Function)environment.invokeMethod(v8PipeClass, "get_group_start_processor",
                    this.tupleTransfer.getGroupTuple(), this.tupleTransfer.getArgumentTuple(), pipeId);

            groupEndProcessor = (V8Function)environment.invokeMethod(v8PipeClass, "get_group_start_processor",
                    this.tupleTransfer.getGroupTuple(), this.tupleTransfer.getArgumentTuple(), pipeId);
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public void emitOutTupleEntries(TupleEntryCollector out) {
        outArgs.toIntArray(jOutArgs,  1);
        outFieldTypes.toIntArray(jOutFieldTypes, jOutFieldTypes.length);
        outNumFieldsPerType.toIntArray(jOutNumFieldsPerType, jOutNumFieldsPerType.length);
        outFieldDataOffsets.toIntArray(jOutFieldDataOffsets, jOutFieldDataOffsets.length);

        final int numberOfTuples = jOutArgs[0];
        final int numFields = jOutFieldTypes.length;

        if (jOutNullMap.length < numberOfTuples * numFields) {
            // TODO make this a bunch of longs?
            jOutNullMap = new boolean[numberOfTuples * numFields];
        }

        outNullMap.toBooleanArray(jOutNullMap, numberOfTuples * numFields);

        // Copy data to java
        for (int typeIdx = 0; typeIdx < jOutNumFieldsPerType.length; typeIdx++) {
            final int numberOfEntries = numberOfTuples * jOutNumFieldsPerType[typeIdx];

            switch (typeIdx) {
                case 0: // INT
                    if (jOutInt.length < numberOfEntries) {
                        jOutInt = new int[numberOfEntries];
                    }

                    outInt.toIntArray(jOutInt, numberOfEntries);
                    break;
                case 1: // LONG
                    if (jOutLong.length < numberOfEntries) {
                        jOutLong = new long[numberOfEntries];
                    }

                    outLong.toLongArray(jOutLong, numberOfEntries);
                    break;
                case 2: // BOOL
                    if (jOutBool.length < numberOfEntries) {
                        jOutBool = new boolean[numberOfEntries];
                    }

                    outBool.toBooleanArray(jOutBool, numberOfEntries);
                    break;
                case 3: // DOUBLE
                    if (jOutDouble.length < numberOfEntries) {
                        jOutDouble = new double[numberOfEntries];
                    }

                    outDouble.toDoubleArray(jOutDouble, numberOfEntries);
                    break;
                case 4: // DATE
                    if (jOutDate.length < numberOfEntries) {
                        jOutDate = new Date[numberOfEntries];
                    }

                    outDate.toDateArray(jOutDate, numberOfEntries);
                    break;
                case 5: // STRING
                    if (jOutString.length < numberOfEntries) {
                        jOutString = new String[numberOfEntries];
                    }

                    outString.toStringArray(jOutString, numberOfEntries);
                    break;
            }
        }

        // TODO try using copy of tuple, if so make sure outnullmap case sets null explicitly

        for (int iTuple = 0; iTuple < numberOfTuples; iTuple++) {
            Tuple tuple = new Tuple();

            for (int iField = 0; iField < numFields; iField++) {
                if (jOutNullMap[iTuple * iField]) {
                    continue;
                }

                final int dataIndex = (numberOfTuples * iTuple) + jOutFieldDataOffsets[iField];

                switch (jOutFieldTypes[iField]) {
                    case 0: // INT
                        tuple.set(iField,  jOutInt[dataIndex]);
                        break;
                    case 1: // LONG
                        tuple.set(iField, jOutLong[dataIndex]);
                        break;
                    case 2: // BOOL
                        tuple.set(iField, jOutBool[dataIndex]);
                        break;
                    case 3: // DOUBLE
                        tuple.set(iField, jOutDouble[dataIndex]);
                        break;
                    case 4: // DATE
                        tuple.set(iField, jOutDate[dataIndex]);
                        break;
                    case 5: // STRING
                        tuple.set(iField, jOutString[dataIndex]);
                        break;
                }
            }

            out.add(new TupleEntry(tuple));
        }
    }

    public void setArgument(TupleEntry argument) {
        this.tupleTransfer.setArgument(argument);
    }

    public void setGroup(TupleEntry group) {
        this.tupleTransfer.setGroup(group);
    }

    public void fireArgumentProcessor() {
        argumentProcessor.invokeVoid();
    }

    public void fireGroupStartProcessor() {
        groupStartProcessor.invokeVoid();
    }

    public void fireGroupEndProcessor() {
        groupEndProcessor.invokeVoid();
    }
}
