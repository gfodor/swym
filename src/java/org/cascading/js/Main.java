package org.cascading.js;

import org.cascading.js.util.Environment;

import javax.script.ScriptException;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws ScriptException, IOException, InterruptedException {
        final Environment env = new Environment();
        env.start(new Environment.EnvironmentArgs(args[1], Environment.EnvironmentArgs.Mode.JOB));
        env.shutdown();
        env.getFactory().run();
    }
}
