package org.cascading.js.util;

import lu.flier.tools.shell.Shell;
import org.apache.commons.io.FileUtils;

import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;

public class Environment {
    public void start(String script) throws IOException, ScriptException {
        Shell shell = new Shell();
        shell.evaluateScript(FileUtils.readFileToString(new File("src/js/r.js")), new String[] { script } );
    }
}
