package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Array;
import lu.flier.script.V8Function;
import lu.flier.script.V8Object;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.util.Environment;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.Date;

public class V8OperationContext {
    // Assume people have at most 64 columns of each type (note V8 will expand arrays)
    private static int DEFAULT_INCOMING_BUFFER_SIZE = 64;

    private V8TupleTransfer tupleTransfer;
    private Environment environment;
    private V8Function argumentProcessor;
    private V8Function groupStartProcessor;
    private V8Function groupEndProcessor;
    private TupleEntryCollector outputEntryCollector;

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
    private boolean readFieldMetadata = false;

    public Environment getEnvironment() {
        return environment;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass, int pipeId, Fields groupingFields, Fields argumentFields, Fields resultFields) {
        this.environment = environment;
        V8ScriptEngine eng = environment.getEngine();

        tupleTransfer = new V8TupleTransfer(environment.getEngine(), groupingFields, argumentFields);
        outInt = eng.createArray(new int[DEFAULT_INCOMING_BUFFER_SIZE]);
        outLong = eng.createArray(new long[DEFAULT_INCOMING_BUFFER_SIZE]);
        outBool = eng.createArray(new boolean[DEFAULT_INCOMING_BUFFER_SIZE]);
        outDouble = eng.createArray(new double[DEFAULT_INCOMING_BUFFER_SIZE]);
        outDate = eng.createArray(new Date[DEFAULT_INCOMING_BUFFER_SIZE]);
        outString = eng.createArray(new String[DEFAULT_INCOMING_BUFFER_SIZE]);
        outFieldTypes = eng.createArray(new int[resultFields.size()]);
        outFieldDataOffsets = eng.createArray(new int[resultFields.size()]);
        outNumFieldsPerType = eng.createArray(jOutNumFieldsPerType);
        outNullMap = eng.createArray(new boolean[resultFields.size()]);
        jOutNullMap = new boolean[resultFields.size()];

        jOutFieldTypes = new int[resultFields.size()];
        jOutFieldDataOffsets = new int[resultFields.size()];

        try {
            environment.invokeMethod(v8PipeClass, "set_pipe_out_buffers",
              eng.createArray(new V8Array[] {  outInt, outLong, outBool, outDouble, outDate, outString, outFieldTypes,
                                               outFieldDataOffsets, outNumFieldsPerType, outNullMap }), pipeId);

            V8Function emitCallback = eng.createFunction(this, "emit");

            argumentProcessor = (V8Function)environment.invokeMethod(v8PipeClass, "get_argument_processor",
                    this.tupleTransfer.getGroupTuple(), this.tupleTransfer.getArgumentTuple(), emitCallback, pipeId);

            eng.compile("var foo = function() { }").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            argumentProcessor = (V8Function)scope.get("foo");

            groupStartProcessor = (V8Function)environment.invokeMethod(v8PipeClass, "get_group_start_processor",
                    this.tupleTransfer.getGroupTuple(), this.tupleTransfer.getArgumentTuple(), emitCallback, pipeId);

            groupEndProcessor = (V8Function)environment.invokeMethod(v8PipeClass, "get_group_end_processor",
                    this.tupleTransfer.getGroupTuple(), this.tupleTransfer.getArgumentTuple(), emitCallback, pipeId);
        } catch (ScriptException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void setOutputEntryCollector(TupleEntryCollector out) {
        if (this.outputEntryCollector != out) {
            this.outputEntryCollector = out;
        }
    }

    public void emit() {
        if (!readFieldMetadata) {
            outFieldTypes.toIntArray(jOutFieldTypes, jOutFieldTypes.length);
            outNumFieldsPerType.toIntArray(jOutNumFieldsPerType, jOutNumFieldsPerType.length);
            outFieldDataOffsets.toIntArray(jOutFieldDataOffsets, jOutFieldDataOffsets.length);
            readFieldMetadata = true;
        }

        final int numFields = jOutFieldTypes.length;

        outNullMap.toBooleanArray(jOutNullMap, numFields);

        // Copy data to java
        for (int typeIdx = 0; typeIdx < jOutNumFieldsPerType.length; typeIdx++) {
            final int numberOfEntries = jOutNumFieldsPerType[typeIdx];
            if (numberOfEntries == 0) continue;

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

        Tuple tuple = Tuple.size(numFields);

        for (int iField = 0; iField < numFields; iField++) {
            if (jOutNullMap[iField]) {
                continue;
            }

            final int dataIndex =  jOutFieldDataOffsets[iField];

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

        outputEntryCollector.add(new TupleEntry(tuple));
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
