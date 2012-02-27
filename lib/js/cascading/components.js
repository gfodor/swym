(function() {
  var __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; },
    __indexOf = Array.prototype.indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  define(["underscore"], function(_) {
    var CoGroup, Each, EachStep, EachTypes, Every, GroupBy, Pipe;
    EachTypes = {
      GENERATOR: 0,
      FILTER: 1
    };
    return {
      EachTypes: EachTypes,
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

        _Class.prototype.to_java = function() {
          var flow, jflows;
          return jflows = (function() {
            var _i, _len, _ref, _results;
            _ref = this.flows;
            _results = [];
            for (_i = 0, _len = _ref.length; _i < _len; _i++) {
              flow = _ref[_i];
              _results.push(flow.to_java());
            }
            return _results;
          }).call(this);
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
          this.assemblies[assembly.name] = assembly;
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

        _Class.prototype.to_java = function() {
          var assembly, jsinks, jsources, jtail_pipes, name, tap, _ref, _ref2, _ref3;
          jsources = {};
          jsinks = {};
          _ref = this.sources;
          for (name in _ref) {
            tap = _ref[name];
            jsources[name] = tap.to_java();
          }
          _ref2 = this.sinks;
          for (name in _ref2) {
            tap = _ref2[name];
            jsinks[name] = tap.to_java();
          }
          jtail_pipes = [];
          _ref3 = this.assemblies;
          for (name in _ref3) {
            assembly = _ref3[name];
            jtail_pipes.unshift(assembly.to_java());
          }
          return Cascading.Factory.Flow(this.name, jsources, jsinks, jtail_pipes);
        };

        return _Class;

      })(),
      Tap: (function() {

        _Class.prototype.is_tap = true;

        function _Class(path, scheme) {
          this.path = path;
          this.scheme = scheme;
          this.outgoing = this.scheme.fields;
          this.is_bound = false;
        }

        _Class.prototype.to_java = function() {
          var jscheme;
          jscheme = this.scheme.to_java();
          return Cascading.Factory.Hfs(jscheme, this.path);
        };

        return _Class;

      })(),
      Assembly: (function() {

        _Class.prototype.is_assembly = true;

        function _Class(name, parent) {
          this.name = name;
          this.flow = parent.is_flow != null ? parent : parent.flow;
          this.source = this.flow.sources[this.name];
          if (!this.source) {
            throw new Error("Unknown source " + this.name + " for assembly");
          }
          if (parent.is_flow != null) {
            new Pipe(this, name);
          } else {
            new Pipe(this, name, parent.tail_pipe);
          }
        }

        _Class.prototype.current_each = function() {
          var _ref;
          return (_ref = this.last_each_pipe) != null ? _ref : this.last_each_pipe = this.add_pipe(new Each(this));
        };

        _Class.prototype.add_pipe = function(pipe) {
          if (pipe.is_each != null) this.last_each_pipe = pipe;
          if (!this.tail_pipe) {
            this.head_pipe = pipe;
            pipe.connect_to_incoming(this.source.outgoing);
          } else {
            pipe.parent_pipe = this.tail_pipe;
            pipe.connect_to_incoming(this.tail_pipe.outgoing);
          }
          return this.tail_pipe = pipe;
        };

        _Class.prototype.to_java = function() {
          return this.tail_pipe.to_java(this.tail_pipe.parent_pipe);
        };

        return _Class;

      })(),
      Pipe: Pipe = (function() {

        Pipe.registerPipeCallback = function(callback, pipe_index, callback_type) {
          var _base;
          if (callback_type == null) callback_type = "default";
          if (Pipe.pipeCallbacks == null) Pipe.pipeCallbacks = {};
          if ((_base = Pipe.pipeCallbacks)[pipe_index] == null) {
            _base[pipe_index] = {};
          }
          return Pipe.pipeCallbacks[pipe_index][callback_type] = callback;
        };

        Pipe.invokePipeCallback = function() {
          var call, callback, callback_type, operation, pipe_index, tupleBuffer;
          pipe_index = arguments[0];
          callback_type = arguments[1];
          tupleBuffer = arguments[2];
          operation = arguments[3];
          call = arguments[4];
          if (callback_type == null) callback_type = "default";
          callback = Pipe.pipeCallbacks[pipe_index][callback_type];
          return callback.apply(Pipe, [tupleBuffer, operation, call]);
        };

        Pipe.getNextPipeIndex = function() {
          if (Pipe.current_pipe_index == null) Pipe.current_pipe_index = 0;
          return Pipe.current_pipe_index += 1;
        };

        Pipe.prototype.is_pipe = true;

        function Pipe(assembly, name, parent_pipe) {
          this.name = name;
          this.parent_pipe = parent_pipe;
          if (this.parent_pipe == null) this.parent_pipe = null;
          this.pipe_index = Pipe.getNextPipeIndex();
          assembly.add_pipe(this);
        }

        Pipe.prototype.connect_to_incoming = function(incoming) {
          return this.incoming = this.outgoing = incoming;
        };

        Pipe.prototype.to_java = function() {
          var parent_jpipe, _ref;
          parent_jpipe = (_ref = this.parent_pipe) != null ? _ref.to_java() : void 0;
          if (parent_jpipe != null) {
            return Cascading.Factory.Pipe(this.name, parent_jpipe);
          } else {
            return Cascading.Factory.Pipe(this.name);
          }
        };

        return Pipe;

      }).call(this),
      Each: Each = (function(_super) {

        __extends(Each, _super);

        Each.prototype.is_each = true;

        function Each(assembly) {
          Each.__super__.constructor.call(this, assembly);
          this.steps = [];
        }

        Each.prototype.add_step = function(step) {
          var tail_step;
          if (step.is_each_step == null) throw new Error("Not an each step");
          tail_step = _.last(this.steps);
          step.connect_to_incoming(tail_step ? tail_step.outgoing : this.incoming);
          this.steps.unshift(step);
          this.outgoing = step.outgoing;
          return step;
        };

        Each.prototype.to_java = function(parent_pipe) {
          var parent_jpipe, _ref;
          if (this.type === EachTypes.GENERATOR) {
            parent_jpipe = (_ref = this.parent_pipe) != null ? _ref.to_java() : void 0;
            return Cascading.Factory.GeneratorEach(this.argument_selector, this.result_fields, Cascading.EnvironmentArgs, this.pipe_index, parent_jpipe);
          }
        };

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
      GroupBy: GroupBy = (function(_super) {

        __extends(GroupBy, _super);

        GroupBy.prototype.is_group_by = true;

        function GroupBy(group_fields, params) {
          var _ref;
          this.group_fields = group_fields;
          if (typeof this.group_fields !== "object") {
            throw new Error("Invalid group by fields " + this.group_fields);
          }
          this.sort_fields = (_ref = params != null ? params.sort_fields : void 0) != null ? _ref : [];
        }

        return GroupBy;

      })(Pipe),
      CoGroup: CoGroup = (function(_super) {

        __extends(CoGroup, _super);

        function CoGroup() {
          CoGroup.__super__.constructor.apply(this, arguments);
        }

        CoGroup.prototype.is_co_group = true;

        return CoGroup;

      })(Pipe),
      EachStep: EachStep = (function() {

        EachStep.prototype.is_each_step = true;

        function EachStep(each, spec, callback) {
          this.spec = spec;
          this.callback = callback;
          each.add_step(this);
        }

        EachStep.prototype.connect_to_incoming = function(incoming) {
          var field, target, _ref, _results;
          this.incoming = incoming;
          this.outgoing = this.incoming.slice(0);
          _ref = this.spec;
          _results = [];
          for (field in _ref) {
            target = _ref[field];
            if (target && !_.include(this.incoming, field)) {
              throw new Error("Invalid field " + field + " being renamed to " + target);
            }
            if (__indexOf.call(this.outgoing, field) >= 0) {
              this.outgoing = _.without(this.outgoing, field);
              if (target != null) {
                _results.push(this.outgoing.push(target));
              } else {
                _results.push(void 0);
              }
            } else {
              _results.push(this.outgoing.push(field));
            }
          }
          return _results;
        };

        return EachStep;

      })()
    };
  });

}).call(this);
