package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import lu.flier.script.V8Array;
import lu.flier.script.V8Function;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.FieldSet;
import org.cascading.js.JSType;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

/**
 * Uber-buffer
 */
public class V8TupleBuffer {
    public static int BUFFER_SIZE = 32 * 1024;

    private V8ScriptEngine eng;

    // Map of [Set][Field Offset] => Type
    final int[][] fieldTypes = new int[FieldSet.values().length][];
    final int[][] fieldTypeCounts = new int[FieldSet.values().length][];
    final int[][] fieldDataOffsets = new int[FieldSet.values().length][];
    final int[][] cascadingFieldOffsets = new int[FieldSet.values().length][];

    // Encoded group sizes.
    // If a group is of size one (very common) then it is run length encoded as [MAX_INT, # of groups of size one]
    // otherwise we just add the group size. The list is terminated by -1.
    final int[] groupSizesRLE = new int[BUFFER_SIZE * 2 + 3];
    int groupSizesRLELength = 0;
    int currentGroupSize = 0;

    // Boolean null bitmasks
    final int[][] jNullMasks = new int[FieldSet.values().length][];

    // Sparse matrices of [Set][Field Offset][Tuple Offset]
    // Deepest entries only filled in for field offsets that are known for this type.
    final int[][][] jIntData = new int[FieldSet.values().length][][];
    final long[][][] jLongData = new long[FieldSet.values().length][][];
    final boolean[][][] jBoolData = new boolean[FieldSet.values().length][][];
    final double[][][] jDoubleData = new double[FieldSet.values().length][][];
    final Date[][][] jDateData = new Date[FieldSet.values().length][][];
    final String[][][] jStringData = new String[FieldSet.values().length][][];

    int[] tupleOffsets = new int[FieldSet.values().length];

    final private V8Array v8GroupSizesRLE;
    final private V8Array[] v8NullMasks = new V8Array[FieldSet.values().length];

    final private int groupSizeRLEJsIndex = FieldSet.values().length;
    final private int nullMaskJsArrayIndex = JSType.values().length;

    // The buffer is a 4-nested array indexed by [w][x][y][z]
    // w - 0 is group data, 1 is arg data, 2 is group rle data
    // x - type, or type.length to get null map, which has a bit for each tuple,field pair
    // y - field offset (or for null map, numeric entry offset)
    // z - tuple offset (invalid index for null map)

    private V8Array v8TupleBuffer;

    // Function to clear the buffer's javascript-side state
    private V8Function clearFunction;

    // Cached V8Arrays Set, iField
    private final V8Array[][] v8DataArrays = new V8Array[FieldSet.values().length][];

    private TupleEntryCollector outCollector;

    public static V8TupleBuffer newInput(V8ScriptEngine eng, Fields resultFields, Map<String, JSType> typeMap) {
        V8TupleBuffer buffer = new V8TupleBuffer(eng, new Fields(), new Fields(), resultFields, typeMap);

        // Initialize v8 arrays of appropriate sizes
        buffer.fillV8Arrays();

        return buffer;
    }

    public static V8TupleBuffer newOutput(V8ScriptEngine eng, Fields groupingFields, Fields argumentFields, Map<String, JSType> typeMap) {
        return new V8TupleBuffer(eng, groupingFields, argumentFields, new Fields(), typeMap);
    }

    private V8TupleBuffer(V8ScriptEngine eng, Fields groupingFields, Fields argumentFields, Fields resultFields, Map<String, JSType> typeMap) {
        this.eng = eng;
        v8GroupSizesRLE = eng.createArray(groupSizesRLE);

        Fields[] fieldsForSet = new Fields[FieldSet.values().length];
        fieldsForSet[FieldSet.GROUP.idx] = groupingFields;
        fieldsForSet[FieldSet.ARGS.idx] = argumentFields;
        fieldsForSet[FieldSet.RESULT.idx] = resultFields;

        V8Array[] outTupleArray = new V8Array[FieldSet.values().length + 1];
        outTupleArray[outTupleArray.length - 1] = v8GroupSizesRLE;

        for (FieldSet set : FieldSet.values()) {
            Fields fields = fieldsForSet[set.idx];
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
        setupAccessors(groupingFields, argumentFields, resultFields, fieldsForSet);

        // Get clear function
        try {
            eng.compile("var __v8Temp = function(b) { b.i_group = 0; b.i_arg = 0; b.i_result = 0; b.i_arg_for_group = 0; " +
                    "b.i_group_rle = 0; b.i_single_group_count = 0; }").eval();

            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            clearFunction = (V8Function)scope.get("__v8Temp");
            eng.eval("delete(__v8Temp);");

        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }

        // Set flush function
        addFunctionToTuple("flushFromV8");

        clear();
    }

    public void setOutCollector(TupleEntryCollector out) {
        if (this.outCollector != out) {
            this.outCollector = out;
        }
    }

    private void addFunctionToTuple(String name) {
        Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);

        try {
            eng.compile("var __v8Temp; var __v8Temp2").eval();

            try {
                scope.put("__v8Temp", eng.createFunction(this, name));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }

            scope.put("__v8Temp2", v8TupleBuffer);
            eng.eval("__v8Temp2." + getJsCompatibleFieldName(name) + " = __v8Temp");
            eng.eval("delete(__v8Temp); delete(__v8Temp2);");

        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    public V8Array getBuffer() {
        return v8TupleBuffer;
    }

    private void setupAccessors(Fields groupingFields, Fields argumentFields, Fields resultFields, Fields[] fieldsForSet) {
        // Setup accessors
        for (FieldSet set : FieldSet.values()) {
            Fields fields = fieldsForSet[set.idx];

            for (int iField = 0; iField < fields.size(); iField++) {
                setTupleDataAccessor(set.idx, fieldTypes[set.idx][iField],
                        fieldDataOffsets[set.idx][iField], iField, (String)fields.get(iField));
            }
        }

        if (resultFields.size() == 0) {
            setTupleAccessor("next_group",
                    "var group_sizes = this[" + groupSizeRLEJsIndex + "]; var i_group_rle = this.i_group_rle; " +
                            "var primary = group_sizes[i_group_rle]; " +
                            "if (primary === 2147483647) { " +
                            "  if (this.i_single_group_count >= group_sizes[i_group_rle + 1] - 1) { " +
                            "    if (group_sizes[i_group_rle + 2] !== -1) { " +
                            "this.i_group += 1; this.i_arg += 1; this.i_group_rle += 2; this.i_single_group_count = 0; this.i_arg_for_group = 0; return true; " +
                            "} else { return false; }" +
                            "  } else { " +
                            "    this.i_group += 1; this.i_arg += 1; this.i_arg_for_group = 0; this.i_single_group_count += 1; return true; " +
                            "  }" +
                            "} else {" +
                            "  if (group_sizes[i_group_rle + 1] !== -1) {" +
                            "    this.i_group += 1; this.i_arg += 1; this.i_group_rle += 1; this.i_arg_for_group = 0; return true; " +
                            "  } else { return false; }" +
                            "} ");

            setTupleAccessor("next_arg",
                    "var i_group_rle = this.i_group_rle; var primary = this[" + groupSizeRLEJsIndex + "][i_group_rle]; " +
                            "if (primary === 2147483647) { return false; }" +
                            "else { " +
                            "  if (primary > this.i_arg_for_group + 1) { " +
                            "    this.i_arg += 1; this.i_arg_for_group += 1; return true; " +
                            "   } else { return false; }" +
                            "} ");
        } else if (resultFields.size() > 0) {
            setTupleAccessor("next_result",
               "this.i_result += 1;" +
               "if (this.i_result >= " + BUFFER_SIZE + ") { " +
               "  this.flush(); " +
               "}"
            );

            setTupleAccessor("flush",
                    "this.flushFromV8(this.i_result); this.i_result = 0; " +
                    "var mask = this[" + FieldSet.RESULT.idx + "][" + nullMaskJsArrayIndex + "];" +
                    "for (var i = 0; i < " + (BUFFER_SIZE / 32) + "; i++) { " +
                    "  mask[i] = 0;" +
                    "}"
            );
        }
    }

    public void fillV8Arrays() {
        groupSizesRLE[groupSizesRLELength] = -1;
        v8GroupSizesRLE.setElements(groupSizesRLE, groupSizesRLELength + 1);

        for (FieldSet set : FieldSet.values()) {
            final int setIdx = set.idx;

            if (fieldTypes[setIdx].length == 0) continue;

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

    public void flushFromV8(Object[] args) {
        int numberOfTuples = (Integer)args[0];
        this.fillJavaArrays(numberOfTuples);

        if (outCollector != null) {
            final int setIdx = FieldSet.RESULT.idx;
            final int[] fieldTypes = this.fieldTypes[setIdx];
            final int[] fieldOffsets = this.cascadingFieldOffsets[setIdx];
            final int[] fieldDataOffsets = this.fieldDataOffsets[setIdx];

            for (int iTuple = 0; iTuple < numberOfTuples; iTuple++) {
                Tuple tuple = Tuple.size(fieldTypes.length);

                for (int iField = 0; iField < fieldTypes.length; iField++) {
                    int fieldOffset = fieldOffsets[iField];
                    int jsType = fieldTypes[iField];
                    int fieldDataOffset = fieldDataOffsets[iField];

                    switch (jsType) {
                        case 0: // INT
                            tuple.set(fieldOffset, jIntData[setIdx][fieldDataOffset][iTuple]);
                            break;
                        case 1: // LONG
                            tuple.set(fieldOffset, jLongData[setIdx][fieldDataOffset][iTuple]);
                            break;
                        case 2: // BOOL
                            tuple.set(fieldOffset, jBoolData[setIdx][fieldDataOffset][iTuple]);
                            break;
                        case 3: // DOUBLE
                            tuple.set(fieldOffset, jDoubleData[setIdx][fieldDataOffset][iTuple]);
                            break;
                        case 4: // DATE
                            tuple.set(fieldOffset, jDateData[setIdx][fieldDataOffset][iTuple]);
                            break;
                        case 5: // STRING
                            tuple.set(fieldOffset, jStringData[setIdx][fieldDataOffset][iTuple]);
                            break;
                    }
                }

                outCollector.add(new TupleEntry(tuple));
            }
        }
    }

    public void fillJavaArrays(int numberOfTuples) {
        for (FieldSet set : FieldSet.values()) {
            final int setIdx = set.idx;

            if (fieldTypes[setIdx].length == 0) continue;

            v8NullMasks[setIdx].toIntArray(jNullMasks[setIdx], numberOfTuples);

            final int[] fieldTypes = this.fieldTypes[setIdx];

            for (int iField = 0; iField < fieldTypes.length; iField++) {
                switch(fieldTypes[iField]) {
                    case 0: // INT
                        v8DataArrays[setIdx][iField].toIntArray(jIntData[setIdx][fieldDataOffsets[setIdx][iField]], numberOfTuples);
                        break;
                    case 1: // LONG
                        v8DataArrays[setIdx][iField].toLongArray(jLongData[setIdx][fieldDataOffsets[setIdx][iField]], numberOfTuples);
                        break;
                    case 2: // BOOL
                        v8DataArrays[setIdx][iField].toBooleanArray(jBoolData[setIdx][fieldDataOffsets[setIdx][iField]], numberOfTuples);
                        break;
                    case 3: // DOUBLE
                        v8DataArrays[setIdx][iField].toDoubleArray(jDoubleData[setIdx][fieldDataOffsets[setIdx][iField]], numberOfTuples);
                        break;
                    case 4: // DATE
                        v8DataArrays[setIdx][iField].toDateArray(jDateData[setIdx][fieldDataOffsets[setIdx][iField]], numberOfTuples);
                        break;
                    case 5: // STRING
                        v8DataArrays[setIdx][iField].toStringArray(jStringData[setIdx][fieldDataOffsets[setIdx][iField]], numberOfTuples);
                        break;
                }
            }
        }
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
        }

        currentGroupSize = 0;
    }

    public void addGroup(final TupleEntry group) {
        closeGroup();
        addData(FieldSet.GROUP, group);
        tupleOffsets[FieldSet.GROUP.idx] += 1;
    }

    public void addArgument(final TupleEntry args) {
        addData(FieldSet.ARGS, args);

        tupleOffsets[FieldSet.ARGS.idx] += 1;
        currentGroupSize += 1;
    }

    public boolean isFullForArguments() {
        return tupleOffsets[FieldSet.ARGS.idx] >= BUFFER_SIZE;
    }

    public void clear() {
        for (FieldSet fs : FieldSet.values()) {
            if (fieldTypes[fs.idx].length > 0) {
                java.util.Arrays.fill(jNullMasks[fs.idx], 0);
            }
        }

        clearFunction.invoke(v8TupleBuffer);
        Arrays.fill(tupleOffsets, 0);
        groupSizesRLELength = 0;
        currentGroupSize = 0;
    }

    private void addData(final FieldSet set, final TupleEntry entry) {
        final int[] fieldOffsets = this.cascadingFieldOffsets[set.idx];
        final int[] fieldTypes  = this.fieldTypes[set.idx];
        final int[][] intData = this.jIntData[set.idx];
        final long[][] longData = this.jLongData[set.idx];
        final boolean[][] boolData = this.jBoolData[set.idx];
        final double[][] doubleData = this.jDoubleData[set.idx];
        final Date[][] dateData = this.jDateData[set.idx];
        final String[][] stringData = this.jStringData[set.idx];
        final int[] nullMask = this.jNullMasks[set.idx];
        final int entryIndex = tupleOffsets[set.idx];

        final int numFields = fieldOffsets.length;

        for (int iField = 0; iField < numFields; iField++) {
            final int jsType = fieldTypes[iField];

            final Object val = entry.get(fieldOffsets[iField]);

            if (val == null)  {
                continue;
            }

            final int fieldOffset = (entryIndex * numFields) + iField;
            final int maskIndex = fieldOffset / 32;
            final int shiftWidth = 31 - fieldOffset % 32;

            nullMask[maskIndex] |= 0x1l << shiftWidth;

            int dataOffset = fieldDataOffsets[set.idx][iField];

            try {
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
            } catch (ClassCastException e) {
                throw new RuntimeException("Error casting field " + iField + " value " + val + " to " + jsType);
            }
        }
    }

    private String getJsSideIndexName(int setIdx) {
        switch (setIdx) {
            case 0:
                return "i_arg";
            case 1:
                return "i_group";
            case 2:
                return "i_result";
        }

        throw new RuntimeException("Invalid setIdx");
    }

    private V8Function setTupleDataAccessor(int setIdx, int typeIdx, int dataOffset, int fieldOffset, String name) {
        int numFields = fieldTypes[setIdx].length;
        String offsetField = getJsSideIndexName(setIdx);

        String setter =
          "val = arguments[0]; " +
          "if (val !== null) { " +
          "  arr[" + nullMaskJsArrayIndex + "][null_mask_index] |= mask;" +
          "  arr[" + typeIdx + "][" + dataOffset + "][i_tuple] = val; " +
          "} else { " +
          "  arr[" + nullMaskJsArrayIndex + "][null_mask_index] &= ~mask; " +
          "} return val;";

        String getter =
          "return ((~arr[" + nullMaskJsArrayIndex + "][null_mask_index]) & mask) === mask ? null : " +
          "       arr[" + typeIdx + "][" + dataOffset + "][i_tuple];";

        return setTupleAccessor(name,
                "var arr = this[" + setIdx + "]; var i_tuple = this." + offsetField + ";" +
                "var null_mask_offset = i_tuple * " + numFields + " + " + fieldOffset + ";" +
                "var null_mask_index = Math.floor(null_mask_offset / 32); var mask = 0x1 << (31 - null_mask_offset % 32);" +
                "if (arguments.length > 0) { " + setter + "} else { " + getter + " }");
    }

    private String getJsCompatibleFieldName(String name) {
        return name.replaceAll(" ", "_");
    }

    private Object setTupleProperty(String name, String value) {
        V8Array tupleArray = this.v8TupleBuffer;

        try {
            eng.compile("var __v8TupleTransferBuffer").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            scope.put("__v8TupleTransferBuffer", tupleArray);
            String script = "__v8TupleTransferBuffer." + getJsCompatibleFieldName(name) +
                    " = " + value + ";";

            Object ret = eng.compile(script).eval();
            eng.eval("delete(__v8TupleTransferBuffer);");
            return ret;
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }
    private V8Function setTupleAccessor(String name, String functionBody) {
        return (V8Function)setTupleProperty(name, "function() { " + functionBody + "}");
    }
}
