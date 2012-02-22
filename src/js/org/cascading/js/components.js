(function() {
  var __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  define(function() {
    var Each, Every, Pipe;
    return {
      Cascade: (function() {

        _Class.prototype.is_cascade = true;

        function _Class() {
          this.flows = [];
        }

        _Class.prototype.add_flow = function(flow) {
          if (flow.is_flow == null) throw new Error("Invalid flow");
          this.flows.unshift(flow);
          return flow;
        };

        return _Class;

      })(),
      Flow: (function() {

        _Class.prototype.is_flow = true;

        function _Class(name) {
          this.name = name;
          this.assemblies = {};
          this.sources = {};
          this.sinks = {};
        }

        _Class.prototype.add_assembly = function(assembly) {
          if (this.assemblies[assembly.name]) {
            throw new Error("Duplicate assembly " + assembly.name);
          }
          if (assembly.is_assembly == null) throw new Error("Not an assembly");
          this.assemblies[assembly.name];
          return assembly;
        };

        _Class.prototype.add_source = function(name, tap) {
          if (this.sources[name]) throw new Error("Duplicate source " + name);
          if (tap.is_tap == null) throw new Error("Not a tap");
          this.sources[name] = tap;
          return tap;
        };

        _Class.prototype.add_sink = function(name, tap) {
          if (this.sinks[name]) throw new Error("Duplicate sink " + name);
          if (tap.is_tap == null) throw new Error("Not a tap");
          this.sinks[name] = tap;
          return tap;
        };

        return _Class;

      })(),
      Tap: (function() {

        _Class.prototype.is_tap = true;

        function _Class(path, type) {
          this.path = path;
          this.type = type;
        }

        return _Class;

      })(),
      Pipe: Pipe = (function() {

        Pipe.prototype.is_pipe = true;

        function Pipe(name, parent_pipe) {
          this.name = name;
          this.parent_pipe = parent_pipe;
        }

        return Pipe;

      })(),
      Each: Each = (function(_super) {

        __extends(Each, _super);

        function Each() {
          Each.__super__.constructor.apply(this, arguments);
        }

        Each.prototype.is_each = true;

        return Each;

      })(Pipe),
      Every: Every = (function(_super) {

        __extends(Every, _super);

        function Every() {
          Every.__super__.constructor.apply(this, arguments);
        }

        Every.prototype.is_every = true;

        return Every;

      })(Pipe),
      Assembly: (function() {

        _Class.prototype.is_assembly = true;

        function _Class(name, parent) {
          this.name = name;
          if (parent.is_flow != null) {
            this.head_pipe = new Pipe(name);
          } else {
            this.head_pipe = new Pipe(name, parent.tail_pipe);
          }
          this.tail_pipe = this.head_pipe;
        }

        _Class.prototype.add_pipe = function(pipe) {
          pipe.parent_pipe = this.tail_pipe;
          return this.tail_pipe = pipe;
        };

        return _Class;

      })()
    };
  });

}).call(this);
