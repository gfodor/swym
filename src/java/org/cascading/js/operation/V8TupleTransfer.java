package org.cascading.js.operation;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Array;
import lu.flier.script.V8ScriptEngine;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.Date;

public class V8TupleTransfer {
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

        static public Type getByIdx(int idx) {
            for (Type t : Type.values()) {
                if (t.idx == idx) {
                    return t;
                }
            }

            return null;
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

    private Fields argumentFields;
    private Fields groupingFields;
    private V8ScriptEngine eng;

    // Map of [Set][Field Offset] => Type
    final int[][] fieldTypes = new int[Set.values().length][];
    final int[][] fieldTypeCounts = new int[Set.values().length][];

    // true once all field types have been identified through the stream
    boolean allFieldsKnown = false;
    final int[] numberOfFieldsKnown = new int[Set.values().length];

    // Offsets into tuple entries for fields.
    final int[][] fieldOffsets = new int[Set.values().length][];

    // Java-side data arrays
    final private int[][] jInts = new int[Set.values().length][];
    final private long[][] jLongs = new long[Set.values().length][];
    final private boolean[][] jBools = new boolean[Set.values().length][];
    final private double[][] jDoubles = new double[Set.values().length][];
    final private Date[][] jDates = new Date[Set.values().length][];
    final private String[][] jStrings = new String[Set.values().length][];
    final private boolean[][] jNullFlags = new boolean[Set.values().length][];

    // V8 side data arrays
    final private V8Array[] v8Tuples = new V8Array[Set.values().length];
    final private V8Array[] v8Ints = new V8Array[Set.values().length];
    final private V8Array[] v8Longs = new V8Array[Set.values().length];
    final private V8Array[] v8Bools = new V8Array[Set.values().length];
    final private V8Array[] v8Doubles = new V8Array[Set.values().length];
    final private V8Array[] v8Dates = new V8Array[Set.values().length];
    final private V8Array[] v8Strings = new V8Array[Set.values().length];
    final private V8Array[] v8NullFlags = new V8Array[Set.values().length];

    // Array of set, i_field -> index into data array to set
    final private int[][] fieldDataOffsets = new int[Set.values().length][];

    final private int v8NullFlagArrayIndex = Type.values().length - 1;

    public V8TupleTransfer(V8ScriptEngine eng, Fields groupingFields, Fields argumentFields) {
        this.argumentFields = argumentFields;
        this.groupingFields = groupingFields;
        this.eng = eng;

        for (int idx : new int[] { Set.ARGS.idx, Set.GROUP.idx }) {
            Fields fields = idx == Set.ARGS.idx ? argumentFields : groupingFields;
            fieldTypes[idx] = new int[fields.size()];
            fieldOffsets[idx] = new int[fields.size()];
            fieldTypeCounts[idx] = new int[Type.values().length];
            fieldDataOffsets[idx] = new int[fields.size()];

            for (int i = 0; i < fields.size(); i++) {
                fieldOffsets[idx][i]= fields.getPos(fields.get(i));
            }

            Arrays.fill(fieldTypes[idx], Type.UNKNOWN.idx);

            // Start j* arrays at length zero, they are resized as needed.
            jInts[idx] = new int[0];
            jLongs[idx] = new long[0];
            jBools[idx] = new boolean[0];
            jDoubles[idx] = new double[0];
            jDates[idx] = new Date[0];
            jStrings[idx] = new String[0];
            jNullFlags[idx] = new boolean[fields.size()];

            v8Ints[idx] = eng.createArray(new int[fields.size()]);
            v8Longs[idx] = eng.createArray(new long[fields.size()]);
            v8Bools[idx] = eng.createArray(new boolean[fields.size()]);
            v8Doubles[idx] = eng.createArray(new double[fields.size()]);
            v8Dates[idx] = eng.createArray(new Date[fields.size()]);
            v8Strings[idx] = eng.createArray(new String[fields.size()]);
            v8NullFlags[idx] = eng.createArray(jNullFlags[idx]);

            v8Tuples[idx] = eng.createArray(new Object[] {
              v8Ints[idx], v8Longs[idx], v8Bools[idx], v8Doubles[idx], v8Dates[idx], v8Strings[idx], v8NullFlags[idx]
            });

            for (Comparable field : fields) {
                setTupleNullAccessor(idx, field.toString());
            }
        }
    }

    private String getJsCompatibleFieldName(String name) {
        return name.replaceAll(" ", "_");
    }

    private void updateFieldMeta(final Set set, final TupleEntry entry) {
        int newFieldsAdded = 0;
        final int[] fieldOffsets = this.fieldOffsets[set.idx];
        final int numFields = fieldOffsets.length;
        final int[] fieldTypes = this.fieldTypes[set.idx];

        for (int i = 0; i < numFields; i++) {
            if (fieldTypes[i] != Type.UNKNOWN.idx) continue;

            Object val = entry.get(fieldOffsets[i]);

            if (val == null) {
                continue;
            }

            Type jsType = jsTypeForObject(val);
            fieldTypes[i] = jsType.idx;

            switch (jsType) {
                case INT:
                    jInts[set.idx] = new int[jInts[set.idx].length + 1];
                    break;
                case LONG:
                    jLongs[set.idx] = new long[jLongs[set.idx].length + 1];
                    break;
                case DOUBLE:
                    jDoubles[set.idx] = new double[jDoubles[set.idx].length + 1];
                    break;
                case BOOL:
                    jBools[set.idx] = new boolean[jBools[set.idx].length + 1];
                    break;
                case STRING:
                    jStrings[set.idx] = new String[jStrings[set.idx].length + 1];
                    break;
                case DATE:
                    jDates[set.idx] = new Date[jDates[set.idx].length + 1];
                    break;
            }

            newFieldsAdded++;
        }

        if (newFieldsAdded > 0) {
            numberOfFieldsKnown[set.idx] += newFieldsAdded;
            Fields fields = set.idx == Set.ARGS.idx ? argumentFields : groupingFields;

            // Reassign fieldDataOffsets and accessors
            int[] fieldDataOffsets = this.fieldDataOffsets[set.idx];
            int[] maxOffsets = new int[Type.values().length];

            for (int i = 0; i < numFields; i++) {
                if (fieldTypes[i] == Type.UNKNOWN.idx) continue;
                fieldDataOffsets[i] = maxOffsets[fieldTypes[i]]++;

                setTupleDataAccessor(set.idx, fieldTypes[i], fieldDataOffsets[i], i, fields.get(i).toString());
            }
        }
    }

    private void setTupleNullAccessor(int setIdx, String name) {
        setTupleAccessor(setIdx, name, "return null;");
    }

    private void setTupleDataAccessor(int setIdx, int typeIdx, int dataOffset, int fieldOffset, String name) {
        setTupleAccessor(setIdx, name,
                "return this[" + this.v8NullFlagArrayIndex + "][" + fieldOffset + "] === true ? null : this[" + typeIdx + "][" + dataOffset + "];");
    }

    private void setTupleAccessor(int setIdx, String name, String functionBody) {
        V8Array tupleArray = this.v8Tuples[setIdx];

        try {
            eng.compile("var __v8TupleTransferBuffer").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            scope.put("__v8TupleTransferBuffer", tupleArray);
            String script = "__v8TupleTransferBuffer." + getJsCompatibleFieldName(name) +
                            " = function() { " + functionBody + " };" +
                            "delete(__v8TupleTransferBuffer);";

            eng.compile(script).eval();
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    public V8Array getGroupTuple() {
        return v8Tuples[Set.GROUP.idx];
    }

    public V8Array getArgumentTuple() {
        return v8Tuples[Set.ARGS.idx];
    }

    public void setGroup(final TupleEntry group) {
        if (!allFieldsKnown) {
            updateFieldMeta(Set.GROUP, group);
            updateAllFieldsKnown();
        }

        addData(Set.GROUP, group);
    }

    public void setArgument(final TupleEntry args) {
        if (!allFieldsKnown) {
            updateFieldMeta(Set.ARGS, args);
            updateAllFieldsKnown();
        }

        addData(Set.ARGS, args);
    }

    private void addData(final Set set, final TupleEntry entry) {
        final int[] fieldOffsets = this.fieldOffsets[set.idx];
        final int[] fieldTypes  = this.fieldTypes[set.idx];
        final int[] fieldDataOffsets = this.fieldDataOffsets[set.idx];

        final int[] jInts = this.jInts[set.idx];
        final long[] jLongs = this.jLongs[set.idx];
        final boolean[] jBools = this.jBools[set.idx];
        final double[] jDoubles = this.jDoubles[set.idx];
        final Date[] jDates = this.jDates[set.idx];
        final String[] jStrings = this.jStrings[set.idx];
        final boolean[] jNullFlags = this.jNullFlags[set.idx];

        final int numFields = fieldOffsets.length;
        boolean changedNullFlags = false;

        for (int i = 0; i < numFields; i++) {
            final int jsType = fieldTypes[i];

            final Object val = entry.get(fieldOffsets[i]);

            if (val == null || jsType == Type.UNKNOWN.idx)  {
                if (!jNullFlags[i]) {
                    changedNullFlags = true;
                    jNullFlags[i] = true;
                }

                continue;
            }

            if (jNullFlags[i]) {
                jNullFlags[i] = false;
                changedNullFlags = true;
            }

            try {
                switch (jsType) {
                    case 0: // INT
                        jInts[fieldDataOffsets[i]] = ((Number)val).intValue();
                        break;
                    case 1: // LONG
                        jLongs[fieldDataOffsets[i]] = ((Number)val).longValue();
                        break;
                    case 2: // BOOL
                        jBools[fieldDataOffsets[i]] = (Boolean)val;
                        break;
                    case 3: // DOUBLE
                        jDoubles[fieldDataOffsets[i]] = ((Number)val).doubleValue();
                        break;
                    case 4: // DATE
                        jDates[fieldDataOffsets[i]] = (Date)val;
                        break;
                    case 5: // STRING
                        jStrings[fieldDataOffsets[i]] = (String)val;
                        break;
                }
            } catch (ClassCastException e) {
                Fields fields = set == Set.ARGS ? argumentFields : groupingFields;
                throw new RuntimeException("Saw tuple with heterogenous types for  " + fields.get(i) + " saw " +
                    val.getClass() + " value " + val + " when expecting type " + Type.getByIdx(jsType));
            }
        }

        if (jInts.length > 0) {
            v8Ints[set.idx].setElements(jInts);
        }

        if (jLongs.length > 0) {
            v8Longs[set.idx].setElements(jLongs);
        }

        if (jBools.length > 0) {
            v8Bools[set.idx].setElements(jBools);
        }

        if (jDoubles.length > 0) {
            v8Doubles[set.idx].setElements(jDoubles);
        }

        if (jDates.length > 0) {
            v8Dates[set.idx].setElements(jDates);
        }

        if (jStrings.length > 0) {
            v8Strings[set.idx].setElements(jStrings);
        }

        if (changedNullFlags) {
            v8NullFlags[set.idx].setElements(jNullFlags);
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

}

