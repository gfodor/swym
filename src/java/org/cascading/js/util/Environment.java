package org.cascading.js.util;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;
import org.ringojs.engine.RhinoEngine;
import org.ringojs.engine.RingoConfiguration;
import org.ringojs.repository.FileRepository;

import javax.script.ScriptException;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;

public class Environment {
    public static class EnvironmentArgs implements Serializable {
        private String script;
        private boolean runTests;

        public EnvironmentArgs(String script, boolean loadTestFramework) {
            this.script = script;
            this.runTests = loadTestFramework;
        }

        public String getScript() {
            return script;
        }

        public boolean isRunTests() {
            return runTests;
        }
    }

    public Factory getFactory() {
        return factory;
    }

    private Factory factory;
    private Context context;
    private Scriptable scope;
    private RhinoEngine engine;

    public void start(EnvironmentArgs args) throws IOException {
        RingoConfiguration config = new RingoConfiguration(new FileRepository("."),
                new String[0], new String[] { "modules" });

        boolean hasPolicy = System.getProperty("java.security.policy") != null;
        boolean productionMode = true;
        config.setPolicyEnabled(hasPolicy);
        config.setOptLevel(productionMode ? 9 : -1);
        config.setDebug(false);
        config.setVerbose(!productionMode);
        config.setParentProtoProperties(false);
        config.setStrictVars(!productionMode);
        config.setReloading(!productionMode);
        config.setMainScript(args.getScript());
        config.setArguments(new String[] { args.getScript() });
        try {
            engine = new RhinoEngine(config, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        context = engine.getContextFactory().enterContext(null);
        factory = new Factory();

        Scriptable envScope = engine.loadModule(context, "cascading/env", null);
        envScope.put("factory", envScope, Context.javaToJS(factory, envScope));
        envScope.put("args", envScope, Context.javaToJS(args, envScope));

        scope = engine.loadModule(context, "r", null);

        if (args.runTests) {
            engine.loadModule(context, "cascading/test/run", null);
        }
    }

    public void shutdown() {
        context = null;
    }

    public Object evaluateScript(String script) throws ScriptException, IOException {
        return context.evaluateString(scope, script, "", 0, null);
    }

    public void invokeMethod(Object target, String name, Object... objects) throws ScriptException, NoSuchMethodException, IOException, ExecutionException, InterruptedException {
        engine.invoke(target, name, objects);
    }

    public void invokeFunction(String name, Object... objects) throws ScriptException, NoSuchMethodException, IOException, ExecutionException, InterruptedException {
        engine.invoke(null, name, objects);
    }

    public Object extractObject(String name) {
        return scope.get(name, scope);
    }
}
