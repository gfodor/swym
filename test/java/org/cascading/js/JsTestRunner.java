package org.cascading.js;

import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public class JsTestRunner {
    public static void main(String[] args) throws ScriptException, IOException, InterruptedException {
        for (final String script : args) {
            final Environment env = new Environment();
            env.start(new Environment.EnvironmentArgs("lib" + script, Environment.EnvironmentArgs.Mode.SPECS));
            env.shutdown();
            //env.getFactory().run();
        }
    }
}
