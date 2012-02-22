(function() {

  define(["./components", "./schemes"], function(components, schemes) {
    return {
      Components: {
        cascade: function(f) {
          this.cascade = new components.Cascade();
          return f(this);
        },
        flow: function(name, f) {
          if (!this.cascade) throw new Error("No cascade created");
          this.flow = this.register(this.cascade.add_flow(new components.Flow(name)));
          return f(this);
        },
        source: function(name, tap) {
          if (!this.flow) throw new Error("No flow created");
          if (this.current_assembly() != null) {
            throw new Error("Cannot define source inside assembly");
          }
          return this.flow.add_source(name, tap);
        },
        sink: function(name, tap) {
          if (!this.flow) throw new Error("No flow created");
          if (this.current_assembly() != null) {
            throw new Error("Cannot define sink inside assembly");
          }
          return this.flow.add_sink(name, tap);
        },
        tap: function(path, scheme) {
          if (scheme.is_scheme == null) {
            throw new Error("Invalid scheme " + scheme.constructor + " vs " + schemes.TextLine);
          }
          return new components.Tap(path, scheme);
        },
        assembly: function(name, f) {
          if (!this.flow) throw new Error("No flow created");
          this.assembly = this.register(this.flow.add_assembly(new components.Assembly(name, this.flow)));
          this.assembly_stack.push(this.assembly);
          f(this);
          return this.assembly_stack.pop();
        },
        current_assembly: function() {
          return this.assembly_stack[0];
        },
        group_by: function(fields, params, f) {
          if (this.is_in_group_by()) throw new Error("Cannot nest group bys");
          if (typeof params === "function" && !(f != null)) f = params;
          if (typeof fields === "string") fields = [fields];
          this.current_group_by = new components.GroupBy(fields, params);
          this.current_assembly().add_pipe(this.current_group_by);
          f();
          return this.current_group_by = null;
        },
        is_in_group_by: function() {
          return this.current_group_by != null;
        }
      },
      EachPipes: {
        insert: function(params) {
          var name, value, _results;
          _results = [];
          for (name in params) {
            value = params[name];
            if (typeof value === 'function') {
              _results.push(this.apply(function(tuple, emitter) {
                return tuple[name] = value(tuple);
              }));
            } else {
              _results.push(this.apply(function(tuple, emitter) {
                return tuple[name] = value;
              }));
            }
          }
          return _results;
        },
        apply: function(callback) {
          var pipe;
          if (this.is_in_group_by()) {
            throw new Error("Cannot define apply pipe inside of group by");
          }
          pipe = new components.Each(components.EachTypes.FUNCTION, callback);
          this.current_assembly().add_pipe(pipe);
          return pipe;
        },
        filter: function(callback) {
          var pipe;
          if (this.is_in_group_by()) {
            throw new Error("Cannot define filter pipe inside of group by");
          }
          pipe = new components.Each(components.EachTypes.FILTER, callback);
          this.current_assembly().add_pipe(pipe);
          return pipe;
        }
      },
      EveryPipes: {
        count: function() {}
      }
    };
  });

}).call(this);
