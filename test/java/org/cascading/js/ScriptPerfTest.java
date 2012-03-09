package org.cascading.js;

import lu.flier.script.V8Array;
import lu.flier.script.V8Function;
import lu.flier.script.V8Object;
import lu.flier.script.V8ScriptEngine;
import org.cascading.js.util.Environment;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.io.IOException;

public class ScriptPerfTest {
    @Test
    public void baselineTest() throws ScriptException, IOException, NoSuchMethodException {
        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs(null, Environment.EnvironmentArgs.Mode.SPECS));
        V8ScriptEngine eng = env.getEngine();

        eng.compile("root = {}; var getter = null; var setter = null; root.foo = function() { if (getter == null) { getter = testObj.getS; setter = testObj.setS; } }; bar = function() { return 10; }").eval();
        Bindings scope = eng.getBindings(ScriptContext.ENGINE_SCOPE);
        V8Object rootObj = (V8Object)scope.get("root");
        FooObj testObj = new FooObj();
        testObj.setS("Hello");
        testObj.setL(123);
        V8Array retBuf = eng.createArray(new Object[] { "hello" });
        scope.put("retBuf", retBuf);

        V8Function f = (V8Function)rootObj.get("foo");

        long t0 = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++) {
            f.invoke();
        }

        System.out.println("Time 1: " + (System.currentTimeMillis() - t0));

        env.shutdown();
    }

    public class FooObj {
        private String s;
        private int l;

        public void setS(String n) {
            this.s = n;
        }

        public void setL(int l) {
            this.l = l;
        }

        public String getS() {
            return s;
        }

        public long getL() {
            return l;
        }

        public Object getSO() {
            return s;
        }

        public Integer getLO() {
            return l;
        }
    }
}

