package org.cascading.js.util;

import lu.flier.Jav8Context;
import org.apache.commons.io.FileUtils;

import javax.script.*;
import java.io.File;
import java.io.IOException;

public class Environment {
    ScriptEngine eng;
    Compilable compiler;

    public void start() throws IOException, ScriptException {
        eng = new ScriptEngineManager().getEngineByName("jav8");
        compiler = (Compilable) eng;

     	Bindings globalScope = this.eng.getBindings(ScriptContext.ENGINE_SCOPE);
        globalScope.put("Jav8Context", new Jav8Context(eng));

        eng.eval("readFile = function(f) { return Jav8Context.readFile(f); }");
        eng.eval("console = { log: function() {  var args = []; for (var i = 0; i < arguments.length; i++) " +
                "{ args[args.length] = arguments[i]; } Jav8Context.print(args); } }");

        eng.eval(FileUtils.readFileToString(new File("src/js/r.js")));
    }
}
