package org.cascading.js.util;

import lu.flier.script.V8Object;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Scriptable;
import org.ringojs.engine.RhinoEngine;
import org.ringojs.engine.RingoConfiguration;
import org.ringojs.repository.FileRepository;

import javax.script.ScriptException;
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

    public void start(EnvironmentArgs args) throws Exception {
        RingoConfiguration config = new RingoConfiguration(new FileRepository("."),
                new String[0], new String[] { "modules" });

        boolean hasPolicy = System.getProperty("java.security.policy") != null;
        boolean productionMode = false;
        config.setPolicyEnabled(hasPolicy);
        config.setOptLevel(productionMode ? 9 : -1);
        config.setDebug(false);
        config.setVerbose(true);
        config.setParentProtoProperties(false);
        config.setStrictVars(!productionMode);
        config.setReloading(!productionMode);
        config.setMainScript(args.getScript());
        config.setArguments(new String[] { args.getScript() });
        RhinoEngine engine = new RhinoEngine(config, null);
        context = engine.getContextFactory().enterContext(null);

        Scriptable scope = null;

        engine.loadModule(context, "r", scope);

        /*factory = new Factory();
        scope.put("_dummy", scope, factory);

        context.evaluateString(scope, "var Cascading = {}; Cascading.Factory = _dummy;", "", 0, null);
        scope.put("_dummy", scope, args);*/

        /*if (args.loadTestFramework) {
            context.evaluateString(scope, "require('lib/js/jasmine')", "", 0, null);
        }*/

        //context.evaluateString(module, FileUtils.readFileToString(new File("lib/js/r.js")), "", 0, null);

        /*if (args.loadTestFramework) {
            //context.evaluateString(scope, FileUtils.readFileToString(new File("test/js/execute.js")), "", 0, null);
        }*/
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
