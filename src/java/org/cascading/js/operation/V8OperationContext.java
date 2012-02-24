package org.cascading.js.operation;

import lu.flier.script.V8Object;
import org.cascading.js.util.Environment;

public class V8OperationContext {
    private Environment environment;
    private V8Object v8PipeClass;

    public Environment getEnvironment() {
        return environment;
    }

    public V8Object getV8PipeClass() {
        return v8PipeClass;
    }

    public V8OperationContext(Environment environment, V8Object v8PipeClass) {
        this.environment = environment;
        this.v8PipeClass = v8PipeClass;
    }
}
