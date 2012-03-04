(function() {
  var __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

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
          return (_ref = this.last_each_pipe) != null ? _ref : this.last_each_pipe = new Each(this);
        };

        _Class.prototype.add_pipe = function(pipe) {
          if (pipe.is_each != null) this.last_each_pipe = pipe;
          if (this.tail_pipe == null) {
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

        Pipe.register_pipe = function(pipe) {
          if (Pipe.current_pipe_id == null) Pipe.current_pipe_id = 0;
          Pipe.current_pipe_id += 1;
          pipe.pipe_id = Pipe.current_pipe_id;
          if (Pipe.pipes == null) Pipe.pipes = {};
          return Pipe.pipes[pipe.pipe_id] = pipe;
        };

        Pipe.process_tuples = function(pipe_id, in_buffer, in_buffer_length, operation, call) {
          var buffer_flush_size, i_field, i_tuple, idx, num_fields, out_buffer, out_buffer_length, pipe, processor, tuple, _ref;
          pipe = Pipe.pipes[pipe_id];
          processor = pipe.processor;
          buffer_flush_size = 8 * 1024;
          out_buffer = new Array(buffer_flush_size + 256);
          out_buffer_length = 0;
          num_fields = pipe.incoming.length;
          if (typeof flush === "undefined" || flush === null) {
            flush = operation.flushFromV8;
          }
          tuple = {};
          for (i_tuple = 0, _ref = in_buffer_length / num_fields; 0 <= _ref ? i_tuple < _ref : i_tuple > _ref; 0 <= _ref ? i_tuple++ : i_tuple--) {
            for (i_field = 0; 0 <= num_fields ? i_field < num_fields : i_field > num_fields; 0 <= num_fields ? i_field++ : i_field--) {
              idx = (i_tuple * num_fields) + i_field;
              tuple[pipe.incoming[i_field]] = in_buffer[idx];
            }
            processor(tuple, function(t) {
              var field, _i, _len, _ref2, _ref3;
              _ref2 = pipe.outgoing;
              for (_i = 0, _len = _ref2.length; _i < _len; _i++) {
                field = _ref2[_i];
                out_buffer[out_buffer_length] = (_ref3 = t[field]) != null ? _ref3 : null;
                out_buffer_length += 1;
              }
              if (out_buffer_length >= buffer_flush_size) {
                flush.apply(operation, [out_buffer, out_buffer_length, call]);
                return out_buffer_length = 0;
              }
            });
          }
          return flush.apply(operation, [out_buffer, out_buffer_length, call]);
        };

        Pipe.prototype.is_pipe = true;

        function Pipe(assembly, name, parent_pipe) {
          this.name = name;
          if (this.parent_pipe == null) this.parent_pipe = parent_pipe;
          Pipe.register_pipe(this);
          assembly.add_pipe(this);
        }

        Pipe.prototype.connect_to_incoming = function(incoming) {
          return this.incoming = this.outgoing = incoming;
        };

        Pipe.prototype.to_java = function() {
          var out, parent_jpipe, _ref;
          parent_jpipe = (_ref = this.parent_pipe) != null ? _ref.to_java() : void 0;
          if (parent_jpipe != null) {
            return out = Cascading.Factory.Pipe(this.name, parent_jpipe);
          } else {
            return out = Cascading.Factory.Pipe(this.name);
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
          this.steps[this.steps.length] = step;
          this.outgoing = step.outgoing;
          this.processor = this.build_processor();
          return step;
        };

        Each.prototype.build_processor = function() {
          var functions, get_writer, writers;
          functions = _.pluck(this.steps, "function");
          writers = [];
          get_writer = function(level, receiver) {
            var current_writer, f, next_writer;
            current_writer = writers[level];
            if (current_writer) return current_writer;
            if (functions.length === 0) return receiver;
            f = functions[level + 1];
            if (f) {
              next_writer = get_writer(level + 1, receiver);
              return writers[level] = function(tuple) {
                return f(tuple, next_writer);
              };
            } else {
              return writers[level] = function(tuple) {
                return receiver(tuple);
              };
            }
          };
          return function(tuple, receiver) {
            return functions[0](tuple, get_writer(0, receiver));
          };
        };

        Each.prototype.to_java = function(parent_pipe) {
          var parent_jpipe, _ref;
          parent_jpipe = (_ref = this.parent_pipe) != null ? _ref.to_java() : void 0;
          return Cascading.Factory.Each(this.incoming, this.outgoing, Cascading.EnvironmentArgs, this.pipe_id, parent_jpipe);
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

        function EachStep(each, spec, _function) {
          this.each = each;
          this.spec = spec;
          this["function"] = _function;
          this.each.add_step(this);
        }

        EachStep.prototype.connect_to_incoming = function(incoming) {
          var field, k, v, _i, _j, _len, _len2, _ref, _ref2, _ref3, _results;
          this.incoming = incoming;
          this.outgoing = this.incoming.slice(0);
          _ref = this.spec;
          for (k in _ref) {
            v = _ref[k];
            if (k !== "add" && k !== "remove") {
              throw new Error("Invalid argument " + k + " for field spec");
            }
          }
          if (this.spec.add != null) {
            _ref2 = this.spec.add;
            for (_i = 0, _len = _ref2.length; _i < _len; _i++) {
              field = _ref2[_i];
              if (!_.include(this.outgoing, field)) {
                this.outgoing[this.outgoing.length] = field;
              }
            }
          }
          if (this.spec.remove != null) {
            _ref3 = this.spec.remove;
            _results = [];
            for (_j = 0, _len2 = _ref3.length; _j < _len2; _j++) {
              field = _ref3[_j];
              if ((this.spec.add != null) && _.include(this.spec.add, field)) {
                throw new Error("Cannot remove field " + field + " being added in same step");
              }
              if (!_.include(this.incoming, field)) {
                throw new Error("Invalid field " + field + " being removed");
              }
              _results.push(this.outgoing = _.without(this.outgoing, field));
            }
            return _results;
          }
        };

        return EachStep;

      })()
    };
  });

}).call(this);
