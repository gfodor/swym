package org.cascading.js.operation;

import org.cascading.js.util.Environment;
import org.mozilla.javascript.NativeFunction;

public class ScriptOperationContext {
    private Environment environment;
    private NativeFunction pipeClass;

    public Environment getEnvironment() {
        return environment;
    }

    public NativeFunction getPipeClass() {
        return pipeClass;
    }

    public ScriptOperationContext(Environment environment, NativeFunction pipeClass) {
        this.environment = environment;
        this.pipeClass = pipeClass;
    }
}
