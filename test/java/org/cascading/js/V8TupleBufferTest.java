package org.cascading.js;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8Function;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.operation.V8TupleTransfer;
import org.cascading.js.util.Environment;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class V8TupleBufferTest {
    @Test
    public void testCallbackPerf() throws ScriptException, IOException {
        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs(null, Environment.EnvironmentArgs.Mode.SPECS));
        V8ScriptEngine eng = env.getEngine();
        Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);

        eng.eval("var thisGuy;");
        scope.put("thisGuy", this);
        eng.eval("var f = thisGuy.callMe; var args = [];");
        eng.eval("var f = function() { }");

        long t0 = System.currentTimeMillis();
        V8Function f = (V8Function)scope.get("f");

        for (int i = 0; i < 5000000; i++) {
            f.invokeVoid();
        }

        System.out.println(System.currentTimeMillis() - t0);

        env.shutdown();
    }

    public static void callMe() {
    }

    @Test
    public void baselineTest() throws ScriptException, IOException {
        Fields groupFields = new Fields("gk_s", "gk_i");
        Fields argFields = new Fields("i", "d", "b", "s");

        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs(null, Environment.EnvironmentArgs.Mode.SPECS));
        V8ScriptEngine eng = env.getEngine();

        V8TupleTransfer b = new V8TupleTransfer(env.getEngine(), groupFields, argFields);
        eng.compile("var group; var arg").eval();
        Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);

        scope.put("group", b.getGroupTuple());
        scope.put("arg", b.getArgumentTuple());

        assertNull(eng.eval("group.gk_s()"));
        assertNull(eng.eval("group.gk_i()"));
        assertNull(eng.eval("arg.i()"));
        assertNull(eng.eval("arg.d()"));
        assertNull(eng.eval("arg.b()"));
        assertNull(eng.eval("arg.s()"));

        setGroup(b, "hello", null);
        assertEquals(eng.eval("group.gk_s()"), "hello");
        assertEquals(eng.eval("group.gk_i()"), null);

        setArgument(b, 123, null, false, null);
        assertEquals(eng.eval("arg.i()"), 123);
        assertEquals(eng.eval("arg.d()"), null);
        assertEquals(eng.eval("arg.b()"), false);
        assertEquals(eng.eval("arg.s()"), null);
        setArgument(b, 456, null, false, "something");
        assertEquals(eng.eval("arg.i()"), 456);
        assertEquals(eng.eval("arg.d()"), null);
        assertEquals(eng.eval("arg.b()"), false);
        assertEquals(eng.eval("arg.s()"), "something");
        setArgument(b, 789, null, false, "another");
        assertEquals(eng.eval("arg.i()"), 789);
        assertEquals(eng.eval("arg.d()"), null);
        assertEquals(eng.eval("arg.b()"), false);
        assertEquals(eng.eval("arg.s()"), "another");
        setArgument(b, 999, 0.3, true, "other");
        assertEquals(eng.eval("arg.i()"), 999);
        assertEquals(eng.eval("arg.d()"), 0.3);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "other");

        setGroup(b, "world", null);
        assertEquals(eng.eval("group.gk_s()"), "world");
        assertEquals(eng.eval("group.gk_i()"), null);
        setArgument(b, 321, 0.44, true, "third");
        assertEquals(eng.eval("arg.i()"), 321);
        assertEquals(eng.eval("arg.d()"), 0.44);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "third");
        setArgument(b, 432, 0.55, true, "fourth");
        assertEquals(eng.eval("arg.i()"), 432);
        assertEquals(eng.eval("arg.d()"), 0.55);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "fourth");

        setGroup(b, "test", 123);
        assertEquals(eng.eval("group.gk_s()"), "test");
        assertEquals(eng.eval("group.gk_i()"), 123);
        setArgument(b, 777, 0.11, null, null);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        setArgument(b, 888, 0.22, true, "sixth");
        assertEquals(eng.eval("arg.i()"), 888);
        assertEquals(eng.eval("arg.d()"), 0.22);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "sixth");

        setGroup(b, "final", 123);
        assertEquals(eng.eval("group.gk_s()"), "final");
        assertEquals(eng.eval("group.gk_i()"), 123);
        setArgument(b, 777, 0.11, null, null);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);

        setGroup(b, "countdown", 123);
        assertEquals(eng.eval("group.gk_s()"), "countdown");
        assertEquals(eng.eval("group.gk_i()"), 123);
        setArgument(b, 777, 0.11, null, null);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);

        setGroup(b, "here", 123);
        assertEquals(eng.eval("group.gk_s()"), "here");
        assertEquals(eng.eval("group.gk_i()"), 123);
        setArgument(b, 777, 0.11, null, null);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);

        setGroup(b, "whee", 123);
        assertEquals(eng.eval("group.gk_s()"), "whee");
        assertEquals(eng.eval("group.gk_i()"), 123);
        setArgument(b, 777, 0.11, null, null);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        setArgument(b, 777, 0.11, null, null);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);

        setGroup(b, "another", 123);
        assertEquals(eng.eval("group.gk_s()"), "another");
        assertEquals(eng.eval("group.gk_i()"), 123);
        setArgument(b, 777, 0.11, null, null);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);

        env.shutdown();

        long t0 = System.currentTimeMillis();
        CompiledScript s = eng.compile("arg.s()");

        for (int i = 0; i < 100000; i++) {
            setArgument(b, 777, 0.11, true, "hello");
        }

        System.out.println(System.currentTimeMillis() - t0);
    }

    private void setGroup(V8TupleTransfer buf, Object... obj) {
        buf.setGroup(new TupleEntry(new Tuple(obj)));
    }

    private void setArgument(V8TupleTransfer buf, Object... obj) {
        buf.setArgument(new TupleEntry(new Tuple(obj)));
    }
}
