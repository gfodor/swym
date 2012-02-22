package org.cascading.js.util;

import lu.flier.tools.shell.Shell;
import org.apache.commons.io.FileUtils;

import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;

public class Environment {
    public void start(String script, boolean loadTestFramework) throws IOException, ScriptException {
        Shell shell = new Shell();

        String[] scriptsToRun = null;

        if (loadTestFramework) {
            shell.evaluateScript(FileUtils.readFileToString(new File("lib/js/jasmine.js")), new String[] {});
        } 

        shell.evaluateScript(FileUtils.readFileToString(new File("lib/js/r.js")), new String[] { script });

        if (loadTestFramework) {
            shell.evaluateScript(FileUtils.readFileToString(new File("test/js/execute.js")), new String[] { });
        }
    }
}
