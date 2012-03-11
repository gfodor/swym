package org.cascading.js;

import org.junit.Test;

import javax.script.ScriptException;
import java.io.IOException;

public class RunTest {
    @Test
    public void runTest() throws ScriptException, IOException, NoSuchMethodException, InterruptedException {
        Main.main(new String[] { "run", "jobs/examples/wordcount.js" });
    }
}

