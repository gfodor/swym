package org.cascading.js;

import org.cascading.js.util.Environment;
import org.junit.Test;

import java.io.IOException;

public class JsTestRunner {
    public static void main(String[] args) throws IOException {
        for (final String script : args) {
            final Environment env = new Environment();
            env.start(new Environment.EnvironmentArgs("test/js/" + script, true));
            env.shutdown();
            env.getFactory().run();
        }
    }

    @Test
    public void runTests() throws IOException {
        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs("test/js/org/cascading/js/builder.js", true));
        env.shutdown();
        env.getFactory().run();
    }
}
