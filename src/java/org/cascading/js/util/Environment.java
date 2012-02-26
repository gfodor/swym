package org.cascading.js.util;

import lu.flier.script.V8Array;
import lu.flier.script.V8Object;
import lu.flier.tools.shell.Shell;
import org.apache.commons.io.FileUtils;

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

    private Shell shell;
    private Factory factory;

    public Factory getFactory() {
        return factory;
    }

    public void start(EnvironmentArgs args) throws IOException, ScriptException {
        shell = new Shell();
        factory = new Factory();

        shell.evaluateScript("var _dummy");
        shell.injectObject("_dummy", factory);
        shell.evaluateScript("var Cascading = {}; Cascading.Factory = _dummy;");
        shell.injectObject("_dummy", args);
        shell.evaluateScript("Cascading.EnvironmentArgs = _dummy;");
        shell.evaluateScript("delete _dummy;");

        if (args.loadTestFramework) {
            shell.evaluateScript(FileUtils.readFileToString(new File("lib/js/jasmine.js")));
        } 

        shell.evaluateScript(FileUtils.readFileToString(new File("lib/js/r.js")), new String[] { args.script });

        if (args.loadTestFramework) {
            shell.evaluateScript(FileUtils.readFileToString(new File("test/js/execute.js")));
        }
    }

    public void shutdown() {
        shell.shutdown();
    }

    public Object evaluateScript(String script) throws ScriptException, IOException {
        return shell.evaluateScript(script);
    }

    public Object invokeMethod(Object target, String name, Object... objects) throws ScriptException, NoSuchMethodException {
        return shell.invokeMethod(target, name, objects);
    }

    public void invokeFunction(String name, Object... objects) throws ScriptException, NoSuchMethodException {
        shell.invokeFunction(name, objects);
    }

    public Object extractObject(String name) {
        return shell.extractObject(name);
    }

    public V8Object createObject() {
        return shell.createObject();
    }

    public V8Array createArray(Object[] elements) {
        return shell.createArray(elements);
    }
}
