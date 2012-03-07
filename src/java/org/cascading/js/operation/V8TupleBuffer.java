package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Array;
import lu.flier.script.V8ScriptEngine;

import java.util.Arrays;
import java.util.Date;

/**
 * Uber-buffer
 */
public class V8TupleBuffer {
    public static int BUFFER_SIZE = 8 * 1024;

    public enum Type {
        UNKNOWN(-1),
        INT(0),
        LONG(1),
        BOOL(2),
        DOUBLE(3),
        DATE(4),
        STRING(5);

        public final int idx;

        Type(final int idx) {
            this.idx = idx;
        }
    }

    public enum Set {
        ARGS(0),
        GROUP(1);

        public final int idx;

        Set(final int idx) {
            this.idx = idx;
        }
    }

    // Map of [Set][Field Offset] => Type
    final int[][] fieldTypes = new int[Set.values().length][];
    final int[][] fieldTypeCounts = new int[Set.values().length][];

    // true once all field types have been identified through the stream
    boolean allFieldsKnown = false;
    final int[] numberOfFieldsKnown = new int[Set.values().length];

    // Delimiter tuple offsets where groups change
    final int[] groupTupleOffsets = new int[BUFFER_SIZE];
    int groupTupleCount = 0;

    // Sparse matrices of [Set][Field Offset][Tuple Offset]
    // Deepest entries only filled in for field offsets that are known for this type.
    final int[][][] intData = new int[Set.values().length][][];
    final long[][][] longData = new long[Set.values().length][][];
    final boolean[][][] boolData = new boolean[Set.values().length][][];
    final double[][][] doubleData = new double[Set.values().length][][];
    final Date[][][] dateData = new Date[Set.values().length][][];
    final String[][][] stringData = new String[Set.values().length][][];

    // Offsets into tuple entries for fields.
    final int[][] fieldOffsets = new int[Set.values().length][];

    int currentTupleOffset = 0;

    // Package arrays
    private V8Array packageArray;
    final private V8Array argFieldTypeArray;
    final private V8Array groupFieldTypeArray;
    final private V8Array groupTupleOffsetArray;

    // Cached V8Arrays Set, Field offset
    private final V8Array[][] v8DataArrays = new V8Array[Set.values().length][];

    // Cached V8Array wrappers, set, type
    private final V8Array[][] v8DataWrapperArrays = new V8Array[Set.values().length][];

    public V8TupleBuffer(V8ScriptEngine eng, Fields groupingFields, Fields argumentFields) {
        clear();

        fieldTypes[Set.ARGS.idx] = new int[argumentFields.size()];
        fieldTypes[Set.GROUP.idx] = new int[groupingFields.size()];
        fieldTypeCounts[Set.ARGS.idx] = new int[Type.values().length];
        fieldTypeCounts[Set.GROUP.idx] = new int[Type.values().length];

        Arrays.fill(fieldTypes[Set.ARGS.idx], Type.UNKNOWN.idx);
        Arrays.fill(fieldTypes[Set.GROUP.idx], Type.UNKNOWN.idx);
        Arrays.fill(fieldTypeCounts[Set.ARGS.idx], 0);
        Arrays.fill(fieldTypeCounts[Set.GROUP.idx], 0);

        fieldOffsets[Set.ARGS.idx] = new int[argumentFields.size()];
        fieldOffsets[Set.GROUP.idx] = new int[groupingFields.size()];

        for (int i = 0; i < argumentFields.size(); i++) {
            fieldOffsets[Set.ARGS.idx][i]= argumentFields.getPos(argumentFields.get(i));
        }

        for (int i = 0; i < groupingFields.size(); i++) {
            fieldOffsets[Set.GROUP.idx][i]= groupingFields.getPos(groupingFields.get(i));
        }

        argFieldTypeArray = eng.createArray(fieldTypes[Set.ARGS.idx]);
        groupFieldTypeArray = eng.createArray(fieldTypes[Set.GROUP.idx]);
        groupTupleOffsetArray = eng.createArray(groupTupleOffsets);
    }

    public void addGroup(TupleEntry group) {
        if (!allFieldsKnown) {
            updateFieldMeta(Set.GROUP, group);
            updateAllFieldsKnown();
        }

        groupTupleOffsets[groupTupleCount] = currentTupleOffset;

        addData(Set.GROUP, group);

        groupTupleCount += 1;
    }

    public void addArgument(TupleEntry args) {
        if (!allFieldsKnown) {
            updateFieldMeta(Set.ARGS, args);
            updateAllFieldsKnown();
        }

        addData(Set.ARGS, args);

        currentTupleOffset += 1;
    }

    public boolean isFull() {
        return currentTupleOffset == BUFFER_SIZE;
    }

    public int getTupleCount() {
        return currentTupleOffset;
    }

    public void clear() {
        java.util.Arrays.fill(groupTupleOffsets, 0);
        currentTupleOffset = 0;
        groupTupleCount = 0;
    }

    public V8Array getPackage(V8ScriptEngine eng) {
        argFieldTypeArray.setElements(fieldTypes[Set.ARGS.idx]);
        groupFieldTypeArray.setElements(fieldTypes[Set.GROUP.idx]);
        groupTupleOffsetArray.setElements(groupTupleOffsets);

        final V8Array groupIntData = getV8DataArrays(eng, Set.GROUP, Type.INT);
        final V8Array groupLongData = getV8DataArrays(eng, Set.GROUP, Type.LONG);
        final V8Array groupDoubleData = getV8DataArrays(eng, Set.GROUP, Type.DOUBLE);
        final V8Array groupBooleanData = getV8DataArrays(eng, Set.GROUP, Type.BOOL);
        final V8Array groupDateData = getV8DataArrays(eng, Set.GROUP, Type.DATE);
        final V8Array groupStringData = getV8DataArrays(eng, Set.GROUP, Type.STRING);

        final V8Array argsIntData = getV8DataArrays(eng, Set.ARGS, Type.INT);
        final V8Array argsLongData = getV8DataArrays(eng, Set.ARGS, Type.LONG);
        final V8Array argsDoubleData = getV8DataArrays(eng, Set.ARGS, Type.DOUBLE);
        final V8Array argsBooleanData = getV8DataArrays(eng, Set.ARGS, Type.BOOL);
        final V8Array argsDateData = getV8DataArrays(eng, Set.ARGS, Type.DATE);
        final V8Array argsStringData = getV8DataArrays(eng, Set.ARGS, Type.STRING);

        if (packageArray == null) {
            packageArray = eng.createArray(
                    new V8Array[] {
                            argFieldTypeArray,
                            groupFieldTypeArray,
                            groupTupleOffsetArray,
                            groupIntData,
                            groupLongData,
                            groupDoubleData,
                            groupBooleanData,
                            groupDateData,
                            groupStringData,
                            argsIntData,
                            argsLongData,
                            argsDoubleData,
                            argsBooleanData,
                            argsDateData,
                            argsStringData,
                    });
        }

        return packageArray;
    }

    private void addData(Set set, TupleEntry entry) {
        final int[] fieldOffsets = this.fieldOffsets[set.idx];

        for (int i = 0; i < fieldOffsets.length; i++) {
            int jsType = fieldTypes[set.idx][i];
            if (jsType == Type.UNKNOWN.idx) continue;

            Object val = entry.get(fieldOffsets[i]);

            if (val == null)  {
                // TODO flip bit somehow?
                continue;
            }

            // Hacky, can't use .idx in switch
            switch (jsType) {
                case 0: // INT
                    intData[set.idx][i][currentTupleOffset] = ((Number)val).intValue();
                    break;
                case 1: // LONG
                    longData[set.idx][i][currentTupleOffset] = ((Number)val).longValue();
                    break;
                case 2: // BOOL
                    boolData[set.idx][i][currentTupleOffset] = (Boolean)val;
                    break;
                case 3: // DOUBLE
                    doubleData[set.idx][i][currentTupleOffset] = ((Number)val).doubleValue();
                    break;
                case 4: // DATE
                    dateData[set.idx][i][currentTupleOffset] = (Date)val;
                    break;
                case 5: // STRING
                    stringData[set.idx][i][currentTupleOffset] = (String)val;
                    break;
            }
        }
    }

    private void updateAllFieldsKnown() {
        allFieldsKnown = (numberOfFieldsKnown[Set.ARGS.idx] == fieldTypes[Set.ARGS.idx].length) &&
                         (numberOfFieldsKnown[Set.GROUP.idx] == fieldTypes[Set.GROUP.idx].length);
    }

    private Type jsTypeForObject(Object obj) {
        if (obj instanceof Short || obj instanceof Integer) {
            return Type.INT;
        } else if (obj instanceof Long) {
            return Type.LONG;
        } else if (obj instanceof Double || obj instanceof Float) {
            return Type.DOUBLE;
        } else if (obj instanceof Boolean) {
            return Type.BOOL;
        } else if (obj instanceof String) {
            return Type.STRING;
        } else if (obj instanceof Date) {
            return Type.DATE;
        }

        throw new RuntimeException("Unsupported java class type: " + obj.getClass().getName());
    }

    private void updateFieldMeta(Set set, TupleEntry entry) {
        int newFieldsAdded = 0;
        final int[] fieldOffsets = this.fieldOffsets[set.idx];

        for (int i = 0; i < fieldOffsets.length; i++) {
            if (fieldTypes[set.idx][i] == Type.UNKNOWN.idx) continue;

            Object val = entry.get(fieldOffsets[i]);
            if (val == null) continue;

            Type jsType = jsTypeForObject(val);
            fieldTypes[set.idx][i] = jsType.idx;
            fieldTypeCounts[set.idx][jsType.idx] += 1;

            switch (jsType) {
                case INT:
                    intData[set.idx][i] = new int[BUFFER_SIZE];
                    break;
                case LONG:
                    longData[set.idx][i] = new long[BUFFER_SIZE];
                    break;
                case DOUBLE:
                    doubleData[set.idx][i] = new double[BUFFER_SIZE];
                    break;
                case BOOL:
                    boolData[set.idx][i] = new boolean[BUFFER_SIZE];
                    break;
                case STRING:
                    stringData[set.idx][i] = new String[BUFFER_SIZE];
                    break;
                case DATE:
                    dateData[set.idx][i] = new Date[BUFFER_SIZE];
                    break;
            }

            newFieldsAdded++;
        }

        if (newFieldsAdded > 0) {
            numberOfFieldsKnown[set.idx] += newFieldsAdded;
        }
    }

    private V8Array getV8DataArrays(V8ScriptEngine eng, Set set, Type type) {
        if (v8DataArrays[set.idx] == null) {
            v8DataArrays[set.idx] = new V8Array[fieldOffsets[set.idx].length];
            v8DataWrapperArrays[set.idx] = new V8Array[Type.values().length];
        }

        V8Array wrapper = v8DataWrapperArrays[set.idx][type.idx];

        if (wrapper == null) {
            wrapper = eng.createArray(new V8Array[fieldOffsets[set.idx].length]);
            v8DataWrapperArrays[set.idx][type.idx] = wrapper;
        }

        for (int i = 0; i < fieldOffsets[set.idx].length; i++) {
            final int jsType = fieldTypes[set.idx][i];
            if (jsType != type.idx) continue;
            final V8Array arr = v8DataArrays[set.idx][i];

            // Hacky, can't use .idx in switch
            switch (jsType) {
                case 0: // INT
                    if (arr != null) {
                        arr.setElements(intData[set.idx][i]);
                    } else {
                        wrapper.set(i, eng.createArray(intData[set.idx][i]));
                    }
                    break;
                case 1: // LONG
                    if (arr != null) {
                        arr.setElements(longData[set.idx][i]);
                    } else {
                        wrapper.set(i, eng.createArray(longData[set.idx][i]));
                    }
                    break;
                case 2: // BOOL
                    if (arr != null) {
                        arr.setElements(boolData[set.idx][i]);
                    } else {
                        wrapper.set(i, eng.createArray(boolData[set.idx][i]));
                    }
                    break;
                case 3: // DOUBLE
                    if (arr != null) {
                        arr.setElements(doubleData[set.idx][i]);
                    } else {
                        wrapper.set(i, eng.createArray(doubleData[set.idx][i]));
                    }
                    break;
                case 4: // DATE
                    if (arr != null) {
                        arr.setElements(dateData[set.idx][i]);
                    } else {
                        wrapper.set(i, eng.createArray(dateData[set.idx][i]));
                    }
                    break;
                case 5: // STRING
                    if (arr != null) {
                        arr.setElements(stringData[set.idx][i]);
                    } else {
                        wrapper.set(i, eng.createArray(stringData[set.idx][i]));
                    }
                    break;
            }
        }

        return wrapper;
    }
}
