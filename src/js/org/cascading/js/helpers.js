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
          return this.flow.add_source(name, tap);
        },
        sink: function(name, tap) {
          if (!this.flow) throw new Error("No flow created");
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
          var _ref;
          if (((_ref = stack[0]) != null ? _ref.is_assembly : void 0) == null) {
            throw new Error("No assembly in scope");
          }
          return this.assembly_stack[0];
        }
      },
      EachPipes: {
        insert: function() {},
        each: function(f) {}
      },
      EveryPipes: {
        count: function() {}
      }
    };
  });

}).call(this);
