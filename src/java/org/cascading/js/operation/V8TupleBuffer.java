package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Array;
import lu.flier.script.V8ScriptEngine;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.Date;
import java.util.Map;

/**
 * Uber-buffer
 */
public class V8TupleBuffer {
    public static int BUFFER_SIZE = 8 * 1024;

    private V8ScriptEngine eng;

    public enum JSType {
        INT(0),
        LONG(1),
        BOOL(2),
        DOUBLE(3),
        DATE(4),
        STRING(5);

        public final int idx;

        JSType(final int idx) {
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
    final int[][] fieldDataOffsets = new int[Set.values().length][];
    final int[][] cascadingFieldOffsets = new int[Set.values().length][];

    // Encoded group sizes.
    // If a group is of size one (very common) then it is run length encoded as [MAX_INT, # of groups of size one]
    // otherwise we just add the group size. The list is terminated by -1.
    final int[] groupSizesRLE = new int[BUFFER_SIZE * 2 + 3];
    int groupSizesRLELength = 0;
    int currentGroupSize = 0;
    int groupCount;

    // Boolean null bitmasks
    final int[][] jNullMasks = new int[Set.values().length][];

    // Sparse matrices of [Set][Field Offset][Tuple Offset]
    // Deepest entries only filled in for field offsets that are known for this type.
    final int[][][] jIntData = new int[Set.values().length][][];
    final long[][][] jLongData = new long[Set.values().length][][];
    final boolean[][][] jBoolData = new boolean[Set.values().length][][];
    final double[][][] jDoubleData = new double[Set.values().length][][];
    final Date[][][] jDateData = new Date[Set.values().length][][];
    final String[][][] jStringData = new String[Set.values().length][][];

    int currentTupleOffset = 0;
    int currentGroupOffset = 0;

    final private V8Array v8GroupSizesRLE;
    final private V8Array[] v8NullMasks = new V8Array[Set.values().length];

    // The buffer is a 4-nested array indexed by [w][x][y][z]
    // w - 0 is group data, 1 is arg data, 2 is group rle data
    // x - type, or type.length to get null map, which has a bit for each tuple,field pair
    // y - field offset (or for null map, numeric entry offset)
    // z - tuple offset (invalid index for null map)

    private V8Array v8TupleBuffer;

    // Cached V8Arrays Set, iField
    private final V8Array[][] v8DataArrays = new V8Array[Set.values().length][];

    public V8TupleBuffer(V8ScriptEngine eng, Fields groupingFields, Fields argumentFields, Map<String, JSType> typeMap) {
        this.eng = eng;
        v8GroupSizesRLE = eng.createArray(groupSizesRLE);

        V8Array[] outTupleArray = new V8Array[Set.values().length];
        outTupleArray[outTupleArray.length - 1] = v8GroupSizesRLE;

        for (Set set : Set.values()) {
            Fields fields = set == Set.GROUP ? groupingFields : argumentFields;
            int numFields = fields.size();
            int setIdx = set.idx;

            fieldTypes[setIdx] = new int[numFields];
            fieldTypeCounts[setIdx] = new int[JSType.values().length];
            fieldDataOffsets[setIdx] = new int[numFields];
            jNullMasks[setIdx] = new int[numFields * BUFFER_SIZE / 32];
            v8NullMasks[setIdx] = eng.createArray(jNullMasks[setIdx]);
            v8DataArrays[setIdx] = new V8Array[numFields];

            cascadingFieldOffsets[setIdx] = new int[numFields];

            for (int i = 0; i < numFields; i++) {
                cascadingFieldOffsets[setIdx][i]= fields.getPos(fields.get(i));
            }

            for (int iField = 0; iField < fields.size(); iField++) {
                String field = (String)fields.get(iField);

                if (!typeMap.containsKey(field)) {
                    throw new RuntimeException("Missing field type info for field " + field);
                }

                JSType fieldType = typeMap.get(field);
                fieldTypes[setIdx][iField] = fieldType.idx;
                fieldDataOffsets[setIdx][iField] = fieldTypeCounts[setIdx][fieldType.idx];
                fieldTypeCounts[setIdx][fieldType.idx]++;
            }

            // Initialize tuple data arrays.
            jIntData[setIdx] = new int[fieldTypeCounts[setIdx][JSType.INT.idx]][];
            jLongData[setIdx] = new long[fieldTypeCounts[setIdx][JSType.LONG.idx]][];
            jBoolData[setIdx] = new boolean[fieldTypeCounts[setIdx][JSType.BOOL.idx]][];
            jDoubleData[setIdx] = new double[fieldTypeCounts[setIdx][JSType.DOUBLE.idx]][];
            jDateData[setIdx] = new Date[fieldTypeCounts[setIdx][JSType.DATE.idx]][];
            jStringData[setIdx] = new String[fieldTypeCounts[setIdx][JSType.STRING.idx]][];

            // Tuple array format: [ints, longs.., strings, nullbits]
            V8Array[] v8DataArrayWrap = new V8Array[JSType.values().length + 1];
            v8DataArrayWrap[v8DataArrayWrap.length - 1] = v8NullMasks[setIdx];
            V8Array[][] v8AllTypeDataArrays = new V8Array[JSType.values().length][];

            for (JSType type : JSType.values()) {
                V8Array[] v8TypeDataArrays = v8AllTypeDataArrays[type.idx] = new V8Array[fieldTypeCounts[setIdx][type.idx]];

                for (int iTypeField = 0; iTypeField < fieldTypeCounts[setIdx][type.idx]; iTypeField++) {
                    switch (type.idx) {
                        case 0: // INT
                            jIntData[setIdx][iTypeField] = new int[BUFFER_SIZE];
                            v8TypeDataArrays[iTypeField] = eng.createArray(jIntData[setIdx][iTypeField]);
                            break;
                        case 1: // LONG
                            jLongData[setIdx][iTypeField] = new long[BUFFER_SIZE];
                            v8TypeDataArrays[iTypeField] = eng.createArray(jLongData[setIdx][iTypeField]);
                            break;
                        case 2: // BOOL
                            jBoolData[setIdx][iTypeField] = new boolean[BUFFER_SIZE];
                            v8TypeDataArrays[iTypeField] = eng.createArray(jBoolData[setIdx][iTypeField]);
                            break;
                        case 3: // DOUBLE
                            jDoubleData[setIdx][iTypeField] = new double[BUFFER_SIZE];
                            v8TypeDataArrays[iTypeField] = eng.createArray(jDoubleData[setIdx][iTypeField]);
                            break;
                        case 4: // DATE
                            jDateData[setIdx][iTypeField] = new Date[BUFFER_SIZE];
                            v8TypeDataArrays[iTypeField] = eng.createArray(jDateData[setIdx][iTypeField]);
                            break;
                        case 5: // STRING
                            jStringData[setIdx][iTypeField] = new String[BUFFER_SIZE];
                            v8TypeDataArrays[iTypeField] = eng.createArray(jStringData[setIdx][iTypeField]);
                            break;
                    }
                }

                v8DataArrayWrap[type.idx] = eng.createArray(v8TypeDataArrays);
            }

            // Easy access to the data arrays for setData
            for (int iField = 0; iField < fields.size(); iField++) {
                v8DataArrays[setIdx][iField] =
                        v8AllTypeDataArrays[fieldTypes[setIdx][iField]][fieldDataOffsets[setIdx][iField]];
            }

            outTupleArray[set.idx] = eng.createArray(v8DataArrayWrap);
        }

        v8TupleBuffer = eng.createArray(outTupleArray);

        // Setup accessors
        for (Set set : Set.values()) {
            Fields fields = set == Set.GROUP ? groupingFields : argumentFields;

            for (int iField = 0; iField < fields.size(); iField++) {
                setTupleDataAccessor(set.idx, fieldTypes[set.idx][iField],
                        fieldDataOffsets[set.idx][iField], iField, (String)fields.get(iField));
            }
        }

        // Offset of group, incremented with each call to nextGroup()
        setTupleProperty("i_group", "0");

        // Offset of arg, incremented with each call to nextArg()
        setTupleProperty("i_arg", "0");

        // Offset of arg within group, incremented with each call to nextArg(), reset on nextGroup()
        setTupleProperty("i_arg_for_group", "0");

        setTupleAccessor("next_group", "this.i_group += 1; this.i_arg += 1; this.i_arg_for_group = 0;");
        setTupleAccessor("next_arg", "this.i_arg += 1; this.i_arg_for_group += 1");

        clear();
    }

    public V8Array getBuffer() {
        return v8TupleBuffer;
    }

    public void fillV8Arrays() {
        groupSizesRLE[groupSizesRLELength] = -1;
        v8GroupSizesRLE.setElements(groupSizesRLE, groupSizesRLELength + 1);

        for (Set set : Set.values()) {
            final int setIdx = set.idx;

            v8NullMasks[setIdx].setElements(jNullMasks[setIdx]);

            final int[] fieldTypes = this.fieldTypes[setIdx];

            for (int iField = 0; iField < fieldTypes.length; iField++) {
                switch(fieldTypes[iField]) {
                    case 0: // INT
                        v8DataArrays[setIdx][iField].setElements(jIntData[setIdx][fieldDataOffsets[setIdx][iField]]);
                        break;
                    case 1: // LONG
                        v8DataArrays[setIdx][iField].setElements(jLongData[setIdx][fieldDataOffsets[setIdx][iField]]);
                        break;
                    case 2: // BOOL
                        v8DataArrays[setIdx][iField].setElements(jBoolData[setIdx][fieldDataOffsets[setIdx][iField]]);
                        break;
                    case 3: // DOUBLE
                        v8DataArrays[setIdx][iField].setElements(jDoubleData[setIdx][fieldDataOffsets[setIdx][iField]]);
                        break;
                    case 4: // DATE
                        v8DataArrays[setIdx][iField].setElements(jDateData[setIdx][fieldDataOffsets[setIdx][iField]]);
                        break;
                    case 5: // STRING
                        v8DataArrays[setIdx][iField].setElements(jStringData[setIdx][fieldDataOffsets[setIdx][iField]]);
                        break;
                }
            }
        }

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
                    if (groupSizesRLELength > 1 && groupSizesRLE[groupSizesRLELength - 2] == Integer.MAX_VALUE) {
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
        addData(Set.GROUP, group);
        currentGroupOffset += 1;
    }

    public void addArgument(final TupleEntry args) {
        addData(Set.ARGS, args);

        currentTupleOffset += 1;
        currentGroupSize += 1;
    }

    public boolean isFull() {
        return currentTupleOffset == BUFFER_SIZE;
    }

    public void clear() {
        java.util.Arrays.fill(jNullMasks[Set.ARGS.idx], 0);
        java.util.Arrays.fill(jNullMasks[Set.GROUP.idx], 0);
        currentTupleOffset = 0;
        currentGroupOffset = 0;
        groupSizesRLELength = 0;
        groupCount = 0;
        currentGroupSize = 0;
    }

    private void addData(final Set set, final TupleEntry entry) {
        final int[] fieldOffsets = this.cascadingFieldOffsets[set.idx];
        final int[] fieldTypes  = this.fieldTypes[set.idx];
        final int[][] intData = this.jIntData[set.idx];
        final long[][] longData = this.jLongData[set.idx];
        final boolean[][] boolData = this.jBoolData[set.idx];
        final double[][] doubleData = this.jDoubleData[set.idx];
        final Date[][] dateData = this.jDateData[set.idx];
        final String[][] stringData = this.jStringData[set.idx];
        final int[] nullMask = this.jNullMasks[set.idx];
        final int entryIndex = set == Set.GROUP ? currentGroupOffset : currentTupleOffset;

        final int numFields = fieldOffsets.length;

        for (int iField = 0; iField < numFields; iField++) {
            final int jsType = fieldTypes[iField];

            final Object val = entry.get(fieldOffsets[iField]);

            if (val == null)  {
                final int fieldOffset = (entryIndex * numFields) + iField;
                final int maskIndex = fieldOffset / 32;
                final int shiftWidth = 32 - fieldOffset % 32;

                nullMask[maskIndex] |= (0x1l << shiftWidth);
                continue;
            }

            int dataOffset = fieldDataOffsets[set.idx][iField];

            // Hacky, can't use .idx in switch
            switch (jsType) {
                case 0: // INT
                    intData[dataOffset][entryIndex] = ((Number)val).intValue();
                    break;
                case 1: // LONG
                    longData[dataOffset][entryIndex] = ((Number)val).longValue();
                    break;
                case 2: // BOOL
                    boolData[dataOffset][entryIndex] = (Boolean)val;
                    break;
                case 3: // DOUBLE
                    doubleData[dataOffset][entryIndex] = ((Number)val).doubleValue();
                    break;
                case 4: // DATE
                    dateData[dataOffset][entryIndex] = (Date)val;
                    break;
                case 5: // STRING
                    stringData[dataOffset][entryIndex] = (String)val;
                    break;
            }
        }
    }

    // The buffer is a 4-nested array indexed by [w][x][y][z]
    // w - 0 is group data, 1 is arg data, 2 is group rle data
    // x - type, or type.length to get null map, which has a bit for each tuple,field pair
    // y - field offset (or for null map, numeric entry offset)
    // z - tuple offset (invalid index for null map)

    private void setTupleNullAccessor(int setIdx, String name) {
        setTupleAccessor(name, "return null;");
    }

    private void setTupleDataAccessor(int setIdx, int typeIdx, int dataOffset, int fieldOffset, String name) {
        int nullMaskArrayIndex = JSType.values().length;
        int numFields = fieldTypes[setIdx].length;
        String offsetField = setIdx == Set.GROUP.idx ? "i_group" : "i_arg";

        setTupleAccessor(name,
                "var i_tuple = this." + offsetField + ";" +
                "var null_mask_offset = i_tuple * " + numFields + " + " + fieldOffset + ";\n" +
                "var null_mask_index = Math.floor(null_mask_offset / 32); var mask = 0x00000001 << (32 - null_mask_offset % 32);\n" +
                "console.log(\"entry:  + this[" + setIdx + "][" + nullMaskArrayIndex + "][\" + null_mask_index + \"] \" + mask);\n" +
                "console.log(\"entry: \" + this[" + setIdx + "][" + nullMaskArrayIndex + "][null_mask_index]);\n" +
                "return this[" + setIdx  + "][" + nullMaskArrayIndex + "][null_mask_index] & mask !== 0 ? null : " +
                        "this[" + setIdx + "][" + typeIdx + "][" + dataOffset + "][i_tuple];\n");
    }

    private String getJsCompatibleFieldName(String name) {
        return name.replaceAll(" ", "_");
    }

    private void setTupleProperty(String name, String value) {
        V8Array tupleArray = this.v8TupleBuffer;

        try {
            eng.compile("var __v8TupleTransferBuffer").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            scope.put("__v8TupleTransferBuffer", tupleArray);
            String script = "__v8TupleTransferBuffer." + getJsCompatibleFieldName(name) +
                    " = " + value + ";" +
                    "delete(__v8TupleTransferBuffer);";

            System.out.println(script);
            eng.compile(script).eval();
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }
    private void setTupleAccessor(String name, String functionBody) {
        setTupleProperty(name, "function() { " + functionBody + "}");
    }
}
