package org.cascading.js.util;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.cascading.js.operation.ScriptFunction;
import org.mozilla.javascript.NativeObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Factory {
    private Flow lastFlow;

    private static Fields asFields(Object[] jsFields) {
        if (jsFields.length == 0) {
            return Fields.ALL;
        }

        Comparable[] fieldNames = new Comparable[jsFields.length];

        for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = (Comparable)jsFields[i];
        }

        return new Fields(fieldNames);
    }

    public TextLine TextLine(Object[] fields) {
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

    public Pipe GeneratorEach(Object[] argumentSelector, Object[] resultFields, Environment.EnvironmentArgs args, Integer pipeIndex, Pipe parent) {
        return new Each(parent, asFields(argumentSelector), new ScriptFunction(asFields(resultFields), args, pipeIndex), asFields(resultFields));
    }

    public Flow Flow(String name, NativeObject jsSources, NativeObject jsSinks, Object[] jsTailPipes) {
        Properties properties = new Properties();
        properties.put("mapred.map.tasks", "1");
        properties.put("mapred.reduce.tasks", "1");

        FlowConnector flowConnector = new FlowConnector(properties);
        Map<String, Tap> sources = new HashMap<String, Tap>();
        Map<String, Tap> sinks = new HashMap<String, Tap>();

        for (Map.Entry<Object, Object> entry : jsSources.entrySet()) {
            sources.put(entry.getKey().toString(), (Tap) entry.getValue());
        }

        for (Map.Entry<Object, Object> entry : jsSinks.entrySet()) {
            sinks.put(entry.getKey().toString(), (Tap)entry.getValue());
        }

        Pipe[] tailPipes = new Pipe[jsTailPipes.length];

        for (int i = 0; i < jsTailPipes.length; i++) {
            tailPipes[i] = (Pipe)jsTailPipes[i];
        }

        Flow flow = flowConnector.connect(name, sources, sinks, tailPipes);
        lastFlow = flow;
        return flow;
    }

    public void run() {
        lastFlow.complete();
    }
}
