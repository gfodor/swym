package org.cascading.js;

import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws ScriptException, IOException {
        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs("jobs" + args[0], true));
        env.shutdown();
    }
}
