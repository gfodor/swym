package org.cascading.js;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.operation.V8TupleBuffer;
import org.cascading.js.util.Environment;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class V8TupleBufferTest {
    private V8ScriptEngine eng;

    @Test
    public void baselineTest() throws ScriptException, IOException {
        Fields groupFields = new Fields("gk_s", "gk_i");
        Fields argFields = new Fields("i", "d", "b", "s", "i2");

        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs(null, Environment.EnvironmentArgs.Mode.SPECS));
        this.eng = env.getEngine();

        Map<String, V8TupleBuffer.JSType> typeMap = new HashMap<String, V8TupleBuffer.JSType>();
        typeMap.put("gk_s", V8TupleBuffer.JSType.STRING);
        typeMap.put("gk_i", V8TupleBuffer.JSType.INT);
        typeMap.put("i", V8TupleBuffer.JSType.INT);
        typeMap.put("d", V8TupleBuffer.JSType.DOUBLE);
        typeMap.put("b", V8TupleBuffer.JSType.BOOL);
        typeMap.put("s", V8TupleBuffer.JSType.STRING);
        typeMap.put("i2", V8TupleBuffer.JSType.INT);
        V8TupleBuffer b = new V8TupleBuffer(env.getEngine(), groupFields, argFields, typeMap);

        eng.compile("var buf; ").eval();
        Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);

        scope.put("group", b.getBuffer());

        /*assertNull(eng.eval("group.gk_s()"));
        assertNull(eng.eval("group.gk_i()"));
        assertNull(eng.eval("arg.i()"));
        assertNull(eng.eval("arg.d()"));
        assertNull(eng.eval("arg.b()"));
        assertNull(eng.eval("arg.s()"));*/

        addGroup(b, "hello", null);
        addArgument(b, 123, null, false, null, 444);
        addArgument(b, 456, null, false, "something", 555);
        addArgument(b, 789, null, false, "another", 666);
        addArgument(b, 999, 0.3, true, "other", null);
        addGroup(b, "world", null);
        addArgument(b, 321, 0.44, true, "third", 111);
        addArgument(b, 432, 0.55, true, "fourth", 444);
        addGroup(b, "test", 123);
        addArgument(b, 777, 0.11, null, null, 444);
        addArgument(b, 888, 0.22, true, "sixth", 444);
        addGroup(b, "final", 123);
        addArgument(b, 777, 0.11, null, null, 444);
        addGroup(b, "countdown", 123);
        addArgument(b, 777, 0.11, null, null, null);
        addGroup(b, "here", 123);
        addArgument(b, 777, 0.11, null, null, 111);
        addGroup(b, "whee", 123);
        addArgument(b, 777, 0.11, null, null, 111);
        addArgument(b, 777, 0.11, null, null, 111);
        addGroup(b, "another", 123);
        addArgument(b, 777, 0.11, null, null, 111);

        b.fillV8Arrays();

        assertEquals(eng.eval("group.gk_s()"), "hello");
        assertEquals(eng.eval("group.gk_i()"), null);
        assertEquals(eng.eval("arg.i()"), 123);
        assertEquals(eng.eval("arg.d()"), null);
        assertEquals(eng.eval("arg.b()"), false);
        assertEquals(eng.eval("arg.s()"), null);
        nextArg(b);
        assertEquals(eng.eval("group.gk_s()"), "hello");
        assertEquals(eng.eval("group.gk_i()"), null);
        assertEquals(eng.eval("arg.i()"), 456);
        assertEquals(eng.eval("arg.d()"), null);
        assertEquals(eng.eval("arg.b()"), false);
        assertEquals(eng.eval("arg.s()"), "something");
        nextArg(b);
        assertEquals(eng.eval("group.gk_s()"), "hello");
        assertEquals(eng.eval("group.gk_i()"), null);
        assertEquals(eng.eval("arg.i()"), 789);
        assertEquals(eng.eval("arg.d()"), null);
        assertEquals(eng.eval("arg.b()"), false);
        assertEquals(eng.eval("arg.s()"), "another");
        nextArg(b);
        assertEquals(eng.eval("group.gk_s()"), "hello");
        assertEquals(eng.eval("group.gk_i()"), null);
        assertEquals(eng.eval("arg.i()"), 999);
        assertEquals(eng.eval("arg.d()"), 0.3);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "other");
        nextGroup(b);

        assertEquals(eng.eval("group.gk_s()"), "world");
        assertEquals(eng.eval("group.gk_i()"), null);
        assertEquals(eng.eval("arg.i()"), 321);
        assertEquals(eng.eval("arg.d()"), 0.44);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "third");
        nextArg(b);
        assertEquals(eng.eval("arg.i()"), 432);
        assertEquals(eng.eval("arg.d()"), 0.55);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "fourth");
        nextGroup(b);

        assertEquals(eng.eval("group.gk_s()"), "test");
        assertEquals(eng.eval("group.gk_i()"), 123);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        nextArg(b);
        assertEquals(eng.eval("arg.i()"), 888);
        assertEquals(eng.eval("arg.d()"), 0.22);
        assertEquals(eng.eval("arg.b()"), true);
        assertEquals(eng.eval("arg.s()"), "sixth");
        nextGroup(b);

        assertEquals(eng.eval("group.gk_s()"), "final");
        assertEquals(eng.eval("group.gk_i()"), 123);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        nextGroup(b);

        assertEquals(eng.eval("group.gk_s()"), "countdown");
        assertEquals(eng.eval("group.gk_i()"), 123);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        nextGroup(b);

        assertEquals(eng.eval("group.gk_s()"), "here");
        assertEquals(eng.eval("group.gk_i()"), 123);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        nextGroup(b);

        assertEquals(eng.eval("group.gk_s()"), "whee");
        assertEquals(eng.eval("group.gk_i()"), 123);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        nextArg(b);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        nextGroup(b);

        assertEquals(eng.eval("group.gk_s()"), "another");
        assertEquals(eng.eval("group.gk_i()"), 123);
        assertEquals(eng.eval("arg.i()"), 777);
        assertEquals(eng.eval("arg.d()"), 0.11);
        assertEquals(eng.eval("arg.b()"), null);
        assertEquals(eng.eval("arg.s()"), null);
        nextArg(b);

        env.shutdown();
    }

    private void nextGroup(V8TupleBuffer buf) {
        try {
            eng.compile("var __v8TupleTransferBuffer").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            scope.put("__v8TupleTransferBuffer", buf.getBuffer());
            eng.compile("__v8TupleTransferBuffer.next_group()").eval();
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    private void nextArg(V8TupleBuffer buf) {
        try {
            eng.compile("var __v8TupleTransferBuffer").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            scope.put("__v8TupleTransferBuffer", buf.getBuffer());
            eng.compile("__v8TupleTransferBuffer.next_arg()").eval();
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasNextArg(V8TupleBuffer buf) {
        try {
            eng.compile("var __v8TupleTransferBuffer").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            scope.put("__v8TupleTransferBuffer", buf.getBuffer());
            return (Boolean)eng.compile("__v8TupleTransferBuffer.has_next_arg()").eval();
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasNextGroup(V8TupleBuffer buf) {
        try {
            eng.compile("var __v8TupleTransferBuffer").eval();
            Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
            scope.put("__v8TupleTransferBuffer", buf.getBuffer());
            return (Boolean)eng.compile("__v8TupleTransferBuffer.has_next_group()").eval();
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    private void addGroup(V8TupleBuffer buf, Object... obj) {
        buf.addGroup(new TupleEntry(new Tuple(obj)));
    }

    private void addArgument(V8TupleBuffer buf, Object... obj) {
        buf.addArgument(new TupleEntry(new Tuple(obj)));
    }
}
