package org.cascading.js;

import org.cascading.js.util.Environment;
import org.junit.Test;

import javax.script.ScriptException;
import java.io.IOException;

public class TestFoo {
    @Test
    public void testfoo() throws ScriptException, IOException {
        Environment env = new Environment();
        env.start();
    }
}
