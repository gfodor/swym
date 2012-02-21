package lu.flier;

import lu.flier.script.V8Array;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import javax.script.Compilable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;

public class Jav8Context {
    private ScriptEngine eng;

    public Jav8Context(ScriptEngine eng) {
        this.eng = eng;
    }

    public boolean exists(String path) {
        return (new File(path)).exists();
    }

    public Object exec(String script) throws ScriptException {
        Compilable compiler = (Compilable) this.eng;
        return compiler.compile(script).eval();
    }

    public void print(V8Array objs) {
        System.out.println(StringUtils.join(objs, ","));
    }

    public String[] getArgs() {
        return new String[] { "src/js/go.js" };
    }

    public String readFile(String file) throws IOException {
        return FileUtils.readFileToString(new File(file));
    }
}
