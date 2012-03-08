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

    // Encoded group sizes.
    // If a group is of size one (very common) then it is run length encoded as [MAX_INT, # of groups of size one]
    // otherwise we just add the group size.
    final int[] groupSizesRLE = new int[BUFFER_SIZE * 2 + 2];
    int groupSizesRLELength = 0;
    int currentGroupSize = 0;
    int groupCount;

    // Boolean null bitmasks
    final int[][] nullMasks = new int[Set.values().length][];

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
    final private V8Array groupSizesRLEArray;
    final private V8Array argNullMask;
    final private V8Array groupNullMask;

    // Cached V8Arrays Set, Field offset
    private final V8Array[][] v8DataArrays = new V8Array[Set.values().length][];

    // Cached V8Array wrappers, set, type
    private final V8Array[][] v8DataWrapperArrays = new V8Array[Set.values().length][];

    public V8TupleBuffer(V8ScriptEngine eng, Fields groupingFields, Fields argumentFields) {
        fieldTypes[Set.ARGS.idx] = new int[argumentFields.size()];
        fieldTypes[Set.GROUP.idx] = new int[groupingFields.size()];
        fieldTypeCounts[Set.ARGS.idx] = new int[Type.values().length];
        fieldTypeCounts[Set.GROUP.idx] = new int[Type.values().length];
        nullMasks[Set.ARGS.idx] = new int[argumentFields.size() * BUFFER_SIZE / 32];
        nullMasks[Set.GROUP.idx] = new int[groupingFields.size() * BUFFER_SIZE / 32];

        Arrays.fill(fieldTypes[Set.ARGS.idx], Type.UNKNOWN.idx);
        Arrays.fill(fieldTypes[Set.GROUP.idx], Type.UNKNOWN.idx);

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
        groupSizesRLEArray = eng.createArray(groupSizesRLE);
        argNullMask = eng.createArray(nullMasks[Set.ARGS.idx]);
        groupNullMask = eng.createArray(nullMasks[Set.GROUP.idx]);

        // Initialize second layer of data arrays
        intData[Set.GROUP.idx] = new int[groupingFields.size()][];
        longData[Set.GROUP.idx] = new long[groupingFields.size()][];
        boolData[Set.GROUP.idx] = new boolean[groupingFields.size()][];
        doubleData[Set.GROUP.idx] = new double[groupingFields.size()][];
        dateData[Set.GROUP.idx] = new Date[groupingFields.size()][];
        stringData[Set.GROUP.idx] = new String[groupingFields.size()][];

        intData[Set.ARGS.idx] = new int[argumentFields.size()][];
        longData[Set.ARGS.idx] = new long[argumentFields.size()][];
        boolData[Set.ARGS.idx] = new boolean[argumentFields.size()][];
        doubleData[Set.ARGS.idx] = new double[argumentFields.size()][];
        dateData[Set.ARGS.idx] = new Date[argumentFields.size()][];
        stringData[Set.ARGS.idx] = new String[argumentFields.size()][];

        clear();
    }

    public void closeGroup() {
        if (currentGroupSize > 0) {
            if (groupSizesRLELength == 0) {
                if (currentGroupSize == 1) {
                    groupSizesRLE[0] = Integer.MAX_VALUE;
                    groupSizesRLE[1] = 1;
                    groupSizesRLELength = 2;
                } else {
                    groupSizesRLE[0] = currentGroupSize;
                    groupSizesRLELength = 1;
                }
            } else {
                if (currentGroupSize == 1) {
                    // Append to run length encoding of size-1 groups if it exists, or create it.
                    if (groupSizesRLE[groupSizesRLELength - 2] == Integer.MAX_VALUE) {
                        groupSizesRLE[groupSizesRLELength - 1] += 1;
                    } else {
                        groupSizesRLE[groupSizesRLELength] = Integer.MAX_VALUE;
                        groupSizesRLE[groupSizesRLELength + 1] = 1;
                        groupSizesRLELength += 2;
                    }
                } else {
                    groupSizesRLE[groupSizesRLELength] = currentGroupSize;
                    groupSizesRLELength += 1;
                }
            }

            groupCount += 1;
        }

        currentGroupSize = 0;
    }

    public void addGroup(final TupleEntry group) {
        if (!allFieldsKnown) {
            updateFieldMeta(Set.GROUP, group);
            updateAllFieldsKnown();
        }

        closeGroup();

        addData(Set.GROUP, group);
    }

    public void addArgument(final TupleEntry args) {
        if (!allFieldsKnown) {
            updateFieldMeta(Set.ARGS, args);
            updateAllFieldsKnown();
        }

        addData(Set.ARGS, args);

        currentTupleOffset += 1;
        currentGroupSize += 1;
    }

    public boolean isFull() {
        return currentTupleOffset == BUFFER_SIZE;
    }

    public int getTupleCount() {
        return currentTupleOffset;
    }

    public int getGroupCount() {
        return groupCount;
    }

    public void clear() {
        java.util.Arrays.fill(nullMasks[Set.ARGS.idx], 0);
        java.util.Arrays.fill(nullMasks[Set.GROUP.idx], 0);
        currentTupleOffset = 0;
        groupSizesRLELength = 0;
        groupCount = 0;
        currentGroupSize = 0;
    }

    public V8Array getPackage(final V8ScriptEngine eng) {
        argFieldTypeArray.setElements(fieldTypes[Set.ARGS.idx]);
        groupFieldTypeArray.setElements(fieldTypes[Set.GROUP.idx]);
        groupSizesRLEArray.setElements(groupSizesRLE, groupSizesRLELength);
        argNullMask.setElements(nullMasks[Set.ARGS.idx]);
        groupNullMask.setElements(nullMasks[Set.GROUP.idx]);

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
                            groupSizesRLEArray,
                            groupFieldTypeArray,
                            groupNullMask,
                            argFieldTypeArray,
                            argNullMask,
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

    private void addData(final Set set, final TupleEntry entry) {
        final int[] fieldOffsets = this.fieldOffsets[set.idx];
        final int[] fieldTypes  = this.fieldTypes[set.idx];
        final int[][] intData = this.intData[set.idx];
        final long[][] longData = this.longData[set.idx];
        final boolean[][] boolData = this.boolData[set.idx];
        final double[][] doubleData = this.doubleData[set.idx];
        final Date[][] dateData = this.dateData[set.idx];
        final String[][] stringData = this.stringData[set.idx];
        final int[] nullMask = this.nullMasks[set.idx];

        final int numFields = fieldOffsets.length;

        for (int i = 0; i < numFields; i++) {
            final int jsType = fieldTypes[i];

            final Object val = entry.get(fieldOffsets[i]);

            if (val == null)  {
                int fieldOffset = (currentTupleOffset * numFields) + i;
                int maskIndex = fieldOffset / 32;
                int shiftWidth = 32 - fieldOffset % 32;

                nullMask[maskIndex] |= (0x1l << shiftWidth);
                continue;
            }

            if (jsType == Type.UNKNOWN.idx) continue;

            // Hacky, can't use .idx in switch
            switch (jsType) {
                case 0: // INT
                    intData[i][currentTupleOffset] = ((Number)val).intValue();
                    break;
                case 1: // LONG
                    longData[i][currentTupleOffset] = ((Number)val).longValue();
                    break;
                case 2: // BOOL
                    boolData[i][currentTupleOffset] = (Boolean)val;
                    break;
                case 3: // DOUBLE
                    doubleData[i][currentTupleOffset] = ((Number)val).doubleValue();
                    break;
                case 4: // DATE
                    dateData[i][currentTupleOffset] = (Date)val;
                    break;
                case 5: // STRING
                    stringData[i][currentTupleOffset] = (String)val;
                    break;
            }
        }
    }

    private void updateAllFieldsKnown() {
        allFieldsKnown = (numberOfFieldsKnown[Set.ARGS.idx] == fieldTypes[Set.ARGS.idx].length) &&
                         (numberOfFieldsKnown[Set.GROUP.idx] == fieldTypes[Set.GROUP.idx].length);
    }

    private Type jsTypeForObject(final Object obj) {
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

    private void updateFieldMeta(final Set set, final TupleEntry entry) {
        int newFieldsAdded = 0;
        final int[] fieldOffsets = this.fieldOffsets[set.idx];
        final int numFields = fieldOffsets.length;
        final int[] fieldTypes = this.fieldTypes[set.idx];

        for (int i = 0; i < numFields; i++) {
            if (fieldTypes[i] != Type.UNKNOWN.idx) continue;

            Object val = entry.get(fieldOffsets[i]);
            if (val == null) continue;

            Type jsType = jsTypeForObject(val);
            fieldTypes[i] = jsType.idx;

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

    private V8Array getV8DataArrays(final V8ScriptEngine eng, final Set set, final Type type) {
        if (v8DataArrays[set.idx] == null) {
            v8DataArrays[set.idx] = new V8Array[fieldOffsets[set.idx].length];
            v8DataWrapperArrays[set.idx] = new V8Array[Type.values().length];
        }

        V8Array wrapper = v8DataWrapperArrays[set.idx][type.idx];

        if (wrapper == null) {
            wrapper = eng.createArray(new V8Array[fieldOffsets[set.idx].length]);
            v8DataWrapperArrays[set.idx][type.idx] = wrapper;
        }

        final int numFields = fieldOffsets[set.idx].length;
        final int[] fieldTypes = this.fieldTypes[set.idx];
        final V8Array[] v8DataArrays = this.v8DataArrays[set.idx];
        final int[][] intData = this.intData[set.idx];
        final long[][] longData = this.longData[set.idx];
        final boolean[][] boolData = this.boolData[set.idx];
        final double[][] doubleData = this.doubleData[set.idx];
        final Date[][] dateData = this.dateData[set.idx];
        final String[][] stringData = this.stringData[set.idx];

        for (int i = 0; i < numFields; i++) {
            final int jsType = fieldTypes[i];
            if (jsType != type.idx) continue;
            V8Array arr = v8DataArrays[i];

            // Hacky, can't use .idx in switch
            switch (jsType) {
                case 0: // INT
                    if (arr != null) {
                        arr.setElements(intData[i]);
                    } else {
                        arr = eng.createArray(intData[i]);
                        v8DataArrays[i] = arr;
                        wrapper.set(i, arr);
                    }
                    break;
                case 1: // LONG
                    if (arr != null) {
                        arr.setElements(longData[i]);
                    } else {
                        arr = eng.createArray(longData[i]);
                        v8DataArrays[i] = arr;
                        wrapper.set(i, arr);
                    }
                    break;
                case 2: // BOOL
                    if (arr != null) {
                        arr.setElements(boolData[i]);
                    } else {
                        arr = eng.createArray(boolData[i]);
                        v8DataArrays[i] = arr;
                        wrapper.set(i, arr);
                    }
                    break;
                case 3: // DOUBLE
                    if (arr != null) {
                        arr.setElements(doubleData[i]);
                    } else {
                        arr = eng.createArray(doubleData[i]);
                        v8DataArrays[i] = arr;
                        wrapper.set(i, arr);
                    }
                    break;
                case 4: // DATE
                    if (arr != null) {
                        arr.setElements(dateData[i]);
                    } else {
                        arr = eng.createArray(dateData[i]);
                        v8DataArrays[i] = arr;
                        wrapper.set(i, arr);
                    }
                    break;
                case 5: // STRING
                    if (arr != null) {
                        arr.setElements(stringData[i]);
                    } else {
                        arr = eng.createArray(stringData[i]);
                        v8DataArrays[i] = arr;
                        wrapper.set(i, arr);
                    }
                    break;
            }
        }

        return wrapper;
    }
}
