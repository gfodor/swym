package org.cascading.js.util;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Buffer;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import lu.flier.script.V8Array;
import lu.flier.script.V8Object;
import org.cascading.js.JSType;
import org.cascading.js.operation.ScriptBuffer;
import org.cascading.js.operation.ScriptFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Factory {
    private Flow lastFlow;

    private Fields asFields(V8Array v8Fields) {
        if (v8Fields.size() == 0) {
            return Fields.ALL;
        }

        return new Fields(v8Fields.toArray(new String[v8Fields.size()]));
    }

    public TextLine TextLine(V8Array fields) {
        return new TextLine(asFields(fields));
    }

    public Hfs Hfs(Scheme scheme, String path) {
        return new Hfs(scheme, path, true);
    }

    public Pipe Pipe(String name) {
        return new Pipe(name);
    }

    public Pipe Pipe(String name, Pipe parent) {
        return new Pipe(name, parent);
    }

    public Pipe Each(V8Array argumentSelector, V8Object argumentTypes, V8Array resultFields, V8Object resultTypes, Environment.EnvironmentArgs args, Integer pipeId, Pipe parent) {
        try {
            return new Each(parent, asFields(argumentSelector),
                    new ScriptFunction(asFields(argumentSelector), asTypeMap(argumentTypes),
                                       asFields(resultFields), asTypeMap(resultTypes),
                                       args, pipeId), Fields.RESULTS);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Pipe GroupByBuffer(V8Array groupingFields, V8Array sortFields,
                              V8Array incoming, V8Object incomingTypes,
                              V8Array outgoing, V8Object outgoingTypes,
                              Environment.EnvironmentArgs args, Integer pipeId, Pipe parent) {
        try {
            Fields groupingFieldsFields = asFields(groupingFields);
            Fields stubFields = new Fields();
            for (int i = 0; i < groupingFieldsFields.size(); i++) {
                stubFields = stubFields.append(new Fields(ScriptBuffer.STUB_FIELD_PREFIX + i));
            }

            GroupBy groupBy = new GroupBy(parent, asFields(groupingFields), asFields(sortFields));

            Buffer buffer = new ScriptBuffer(asFields(groupingFields), asFields(incoming), asTypeMap(incomingTypes),
                                             asFields(outgoing).subtract(groupingFieldsFields).append(stubFields),
                                             asTypeMap(outgoingTypes), args, pipeId);

            Pipe prev = new Every(groupBy, asFields(incoming), buffer, asFields(outgoing).append(stubFields));

            Fields currentFields = asFields(outgoing).append(stubFields);

            // Remove grouping fields
            for (int i = 0; i < groupingFieldsFields.size(); i++) {
                Fields newFields = currentFields.subtract(new Fields(groupingFieldsFields.get(i)));
                prev = new Each(prev, newFields, new Identity(), Fields.RESULTS);
                currentFields = newFields;
            }

            // Rename stub fields back to grouping key fields.
            for (int i = 0; i < groupingFieldsFields.size(); i++) {
               prev = new Rename(prev, new Fields(ScriptBuffer.STUB_FIELD_PREFIX + i), new Fields(groupingFieldsFields.get(i)));
            }

            return prev;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Flow Flow(String name, V8Object v8Sources, V8Object v8Sinks, V8Array v8TailPipes) {
        Properties properties = new Properties();
        //properties.setProperty("mapred.map.tasks", "1");
        //properties.setProperty("mapred.reduce.tasks", "1");

        FlowConnector flowConnector = new FlowConnector(properties);
        Map<String, Tap> sources = new HashMap<String, Tap>();
        Map<String, Tap> sinks = new HashMap<String, Tap>();

        for (Map.Entry<String,Object> entry : v8Sources.entrySet()) {
            sources.put(entry.getKey(), (Tap)entry.getValue());
        }

        for (Map.Entry<String,Object> entry : v8Sinks.entrySet()) {
            sinks.put(entry.getKey(), (Tap)entry.getValue());
        }

        Pipe[] tailPipes = new Pipe[v8TailPipes.size()];

        for (int i = 0; i < v8TailPipes.size(); i++) {
            tailPipes[i] = (Pipe)v8TailPipes.get(i);
        }

        Flow flow = flowConnector.connect(name, sources, sinks, tailPipes);
        lastFlow = flow;

        return flow;
    }

    public void run() {
        lastFlow.complete();
    }

    private Map<String, JSType> asTypeMap(V8Object object) {
        Map<String, JSType> map = new HashMap<String, JSType>();

        for (String name : object.keySet()) {
            JSType type = JSType.forIdx((Integer)object.get(name));
            map.put(name, type);
        }

        return map;
    }
}
