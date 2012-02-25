package org.cascading.js.util;

import lu.flier.script.V8Object;
import org.apache.commons.io.FileUtils;
import sun.org.mozilla.javascript.internal.Context;
import sun.org.mozilla.javascript.internal.Scriptable;

import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;

public class Environment {
    public static class EnvironmentArgs implements Serializable {
        private String script;
        private boolean loadTestFramework;

        public EnvironmentArgs(String script, boolean loadTestFramework) {
            this.script = script;
            this.loadTestFramework = loadTestFramework;
        }

        public String getScript() {
            return script;
        }

        public boolean isLoadTestFramework() {
            return loadTestFramework;
        }
    }

    private Factory factory;
    private Context context;
    private Scriptable scope;

    public Factory getFactory() {
        return factory;
    }

    public void start(EnvironmentArgs args) throws IOException, ScriptException {
        context = Context.enter();
        scope = context.initStandardObjects();
        factory = new Factory();
        scope.put("_dummy", scope, factory);

        context.evaluateString(scope, "var Cascading = {}; Cascading.Factory = _dummy;", "", 0, null);
        scope.put("_dummy", scope, args);

        if (args.loadTestFramework) {
            context.evaluateString(scope, "require('lib/js/jasmine')", "", 0, null);
        }

        context.evaluateString(scope, FileUtils.readFileToString(new File("lib/js/r.js")), "", 0, null);

        if (args.loadTestFramework) {
            //context.evaluateString(scope, FileUtils.readFileToString(new File("test/js/execute.js")), "", 0, null);
        }
    }

    public void shutdown() {
        context = null;
        scope = null;
    }

    public Object evaluateScript(String script) throws ScriptException, IOException {
        return null ;//shell.evaluateScript(script);
    }

    public void invokeMethod(Object target, String name, Object... objects) throws ScriptException, NoSuchMethodException {
        //shell.invokeMethod(target, name, objects);
    }

    public void invokeFunction(String name, Object... objects) throws ScriptException, NoSuchMethodException {
        //shell.invokeFunction(name, objects);
    }

    public Object extractObject(String name) {
        return null; //shell.extractObject(name);
    }

    public V8Object createObject() {
        return null; //shell.createObject();
    }
}
