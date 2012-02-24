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
import lu.flier.script.V8Array;
import lu.flier.script.V8Object;
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

    public Pipe GeneratorEach(V8Array argumentSelector, V8Array resultFields, Environment.EnvironmentArgs args, Integer pipeIndex, Pipe parent) {
        return new Each(parent, asFields(argumentSelector), new ScriptFunction(asFields(resultFields), args, pipeIndex), Fields.ALL);
    }

    public Flow Flow(String name, V8Object v8Sources, V8Object v8Sinks, V8Array v8TailPipes) {
        Properties properties = new Properties();
        properties.setProperty("mapred.map.tasks", "1");
        properties.setProperty("mapred.reduce.tasks", "1");

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

    public void run() throws InterruptedException {
        lastFlow.complete();
    }
}
