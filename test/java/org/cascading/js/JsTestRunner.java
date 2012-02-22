package org.cascading.js;

import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public class JsTestRunner {
    public static void main(String[] args) throws ScriptException, IOException {
        for (String script : args) {
            Environment env = new Environment();
            env.start("test/js/" + script, true);
        }
    }
}
