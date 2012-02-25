package org.cascading.js;

import org.cascading.js.util.Environment;
import org.junit.Test;

import javax.script.ScriptException;
import java.io.IOException;

public class JsTestRunner {
    public static void main(String[] args) throws ScriptException, IOException, InterruptedException {
        for (final String script : args) {
            final Environment env = new Environment();
            env.start(new Environment.EnvironmentArgs("test/js/" + script, true));
            env.shutdown();
            env.getFactory().run();
        }
    }

    @Test
    public void runTests() throws ScriptException, IOException, InterruptedException {
        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs("test/js/org/cascading/js/builder.js", true));
        env.shutdown();
        env.getFactory().run();
    }
}
