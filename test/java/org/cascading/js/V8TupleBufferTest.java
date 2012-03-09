package org.cascading.js;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Array;
import org.cascading.js.operation.V8TupleBuffer;
import org.cascading.js.util.Environment;
import org.junit.Test;

import javax.script.ScriptException;
import java.io.IOException;

public class V8TupleBufferTest {
    @Test
    public void baselineTest() throws ScriptException, IOException {
        Fields groupFields = new Fields("gk_s", "gk_i");
        Fields argFields = new Fields("i", "d", "b", "s");

        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs(null, Environment.EnvironmentArgs.Mode.SPECS));

        V8TupleBuffer b = new V8TupleBuffer(env.getEngine(), groupFields, argFields);

        addGroup(b, "hello", null);
        addArgument(b, 123, null, false, null);
        addArgument(b, 456, null, false, "something");
        addArgument(b, 789, null, false, "another");
        addArgument(b, 999, 0.3, true, "other");
        b.closeGroup();
        addGroup(b, "world", null);
        addArgument(b, 321, 0.44, true, "third");
        addArgument(b, 432, 0.55, true, "fourth");
        b.closeGroup();
        addGroup(b, "test", 123);
        addArgument(b, 777, 0.11, null, null);
        addArgument(b, 888, 0.22, true, "sixth");
        b.closeGroup();
        addGroup(b, "final", 123);
        addArgument(b, 777, 0.11, null, null);
        b.closeGroup();
        addGroup(b, "countdown", 123);
        addArgument(b, 777, 0.11, null, null);
        b.closeGroup();
        addGroup(b, "here", 123);
        addArgument(b, 777, 0.11, null, null);
        b.closeGroup();
        addGroup(b, "whee", 123);
        addArgument(b, 777, 0.11, null, null);
        addArgument(b, 777, 0.11, null, null);
        b.closeGroup();
        addGroup(b, "another", 123);
        addArgument(b, 777, 0.11, null, null);
        b.closeGroup();

        V8Array pkg = b.getPackage(env.getEngine());
        b.clear();

        addGroup(b, "hello", null);
        addArgument(b, 123, null, false, null);
        addArgument(b, 456, null, false, "something");

        pkg = b.getPackage(env.getEngine());

        env.shutdown();
    }

    private void addGroup(V8TupleBuffer buf, Object... obj) {
        buf.addGroup(new TupleEntry(new Tuple(obj)));
    }

    private void addArgument(V8TupleBuffer buf, Object... obj) {
        buf.addArgument(new TupleEntry(new Tuple(obj)));
    }
}
