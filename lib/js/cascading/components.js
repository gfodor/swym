(function() {
  var __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  define(["underscore", "./util"], function(_, U) {
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
          this.outgoing_types = this.scheme.field_types;
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
            pipe.connect_to_incoming(this.source);
          } else {
            pipe.parent_pipe = this.tail_pipe;
            pipe.connect_to_incoming(this.tail_pipe);
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

        Pipe.set_pipe_out_buffers = function(out_buffers, pipe_id) {
          var current_offsets, field, i_field, idx, num_types, offset, out_field_data_offsets, out_field_types, out_num_fields_per_type, pipe, type, type_idx, writable_fields, _fn, _ref, _results;
          pipe = Pipe.pipes[pipe_id];
          pipe.out_buffers = out_buffers;
          num_types = _.keys(U.type_idx_map).length;
          out_field_types = out_buffers[num_types];
          out_field_data_offsets = out_buffers[num_types + 1];
          out_num_fields_per_type = out_buffers[num_types + 2];
          current_offsets = [];
          pipe.out_obj = {};
          _ref = U.type_idx_map;
          for (type in _ref) {
            idx = _ref[type];
            out_num_fields_per_type[idx] = 0;
            current_offsets[current_offsets.length] = 0;
          }
          writable_fields = pipe.outgoing;
          if (pipe.group_fields) {
            writable_fields = _.difference(writable_fields, pipe.group_fields);
          }
          _fn = function(type_idx, offset) {
            return pipe.out_obj[field] = function(val) {
              return pipe.out_buffers[type_idx][offset] = val;
            };
          };
          _results = [];
          for (i_field in writable_fields) {
            field = writable_fields[i_field];
            type_idx = pipe.outgoing_types[field];
            out_num_fields_per_type[type_idx] += 1;
            offset = current_offsets[type_idx];
            out_field_data_offsets[i_field] = offset;
            out_field_types[i_field] = type_idx;
            _fn(type_idx, offset);
            _results.push(current_offsets[type_idx] += 1);
          }
          return _results;
        };

        Pipe.get_flush_routine = function(in_buffer, out_buffer, pipe_id) {
          var current_group, finalizer, group_fields, initialized, initializer, non_group_in_fields, non_group_out_fields, num_group_fields, pipe, processor, stub_fields, tuple, writer, _i, _ref, _ref2, _ref3, _ref4, _results;
          pipe = Pipe.pipes[pipe_id];
          initializer = (_ref = pipe.initializer) != null ? _ref : function() {};
          processor = (_ref2 = pipe.processor) != null ? _ref2 : function() {};
          finalizer = (_ref3 = pipe.finalizer) != null ? _ref3 : function() {};
          initialized = false;
          group_fields = (_ref4 = pipe.group_fields) != null ? _ref4 : [];
          non_group_in_fields = _.difference(pipe.incoming, group_fields);
          non_group_out_fields = _.difference(pipe.outgoing, group_fields);
          num_group_fields = group_fields.length;
          stub_fields = _.map((function() {
            _results = [];
            for (var _i = 0; 0 <= num_group_fields ? _i < num_group_fields : _i > num_group_fields; 0 <= num_group_fields ? _i++ : _i--){ _results.push(_i); }
            return _results;
          }).apply(this), function(i) {
            return "___swym_stub_gk_" + i;
          });
          current_group = {};
          tuple = {};
          writer = function(tuple) {
            var field, i_group_field, _j, _len;
            for (i_group_field = 0; 0 <= num_group_fields ? i_group_field < num_group_fields : i_group_field > num_group_fields; 0 <= num_group_fields ? i_group_field++ : i_group_field--) {
              out_buffer[stub_fields[i_group_field]](current_group[group_fields[i_group_field]]);
            }
            for (_j = 0, _len = non_group_out_fields.length; _j < _len; _j++) {
              field = non_group_out_fields[_j];
              out_buffer[field](tuple[field]);
            }
            return out_buffer.next_result();
          };
          return function(last_group_is_complete, input_is_complete) {
            var field, group_field, has_another_group, val, _j, _k, _len, _len2;
            while (true) {
              for (_j = 0, _len = group_fields.length; _j < _len; _j++) {
                group_field = group_fields[_j];
                val = in_buffer[group_field]();
                tuple[group_field] = current_group[group_field] = val;
              }
              while (true) {
                for (_k = 0, _len2 = non_group_in_fields.length; _k < _len2; _k++) {
                  field = non_group_in_fields[_k];
                  tuple[field] = in_buffer[field]();
                }
                if (!initialized) {
                  initializer(tuple);
                  initialized = true;
                }
                processor(tuple, writer);
                if (!in_buffer.next_arg()) break;
              }
              has_another_group = in_buffer.next_group();
              if (has_another_group || last_group_is_complete) {
                finalizer(tuple, writer);
                initializer(tuple);
              }
              if (!has_another_group) break;
            }
            if (input_is_complete) return out_buffer.flush();
          };
        };

        Pipe.prototype.is_pipe = true;

        function Pipe(assembly, name, parent_pipe) {
          this.name = name;
          if (this.parent_pipe == null) this.parent_pipe = parent_pipe;
          Pipe.register_pipe(this);
          assembly.add_pipe(this);
        }

        Pipe.prototype.connect_to_incoming = function(source) {
          this.incoming = this.outgoing = source.outgoing;
          return this.incoming_types = this.outgoing_types = source.outgoing_types;
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
          step.connect_to_incoming(tail_step ? tail_step : this);
          this.steps[this.steps.length] = step;
          this.outgoing = step.outgoing;
          this.outgoing_types = step.outgoing_types;
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
          return Cascading.Factory.Each(this.incoming, this.incoming_types, this.outgoing, this.outgoing_types, Cascading.EnvironmentArgs, this.pipe_id, parent_jpipe);
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

        function GroupBy(assembly, group_fields, spec, initializer, processor, finalizer) {
          var _ref, _ref2;
          this.group_fields = group_fields;
          this.spec = spec;
          this.initializer = initializer;
          this.processor = processor;
          this.finalizer = finalizer;
          GroupBy.__super__.constructor.call(this, assembly);
          if (typeof this.group_fields !== "object") {
            throw new Error("Invalid group by fields " + this.group_fields);
          }
          this.sort_fields = (_ref = (_ref2 = this.spec) != null ? _ref2.sort_fields : void 0) != null ? _ref : [];
        }

        GroupBy.prototype.connect_to_incoming = function(source) {
          var add_field, add_type, group_field, k, type_idx, v, _i, _len, _ref, _ref2, _ref3, _results;
          this.incoming = source.outgoing;
          this.incoming_types = source.outgoing_types;
          this.outgoing = this.group_fields.slice(0);
          this.outgoing_types = {};
          _ref = this.group_fields;
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            group_field = _ref[_i];
            if (!_.include(this.incoming, group_field)) {
              throw new Error("No such field " + group_field + " in incoming");
            }
            if (this.incoming_types[group_field] == null) {
              throw new Error("No such field " + group_field + " in incoming types");
            }
            this.outgoing_types[group_field] = this.incoming_types[group_field];
          }
          _ref2 = this.spec;
          for (k in _ref2) {
            v = _ref2[k];
            if (k !== "add" && k !== "sort" && k !== "types") {
              throw new Error("Invalid argument " + k + " for group by spec");
            }
          }
          _ref3 = this.spec.add;
          _results = [];
          for (add_field in _ref3) {
            add_type = _ref3[add_field];
            type_idx = U.type_idx_map[add_type];
            if (type_idx == null) {
              throw new Error("Invalid type " + add_type + " for " + add_field);
            }
            this.outgoing[this.outgoing.length] = add_field;
            _results.push(this.outgoing_types[add_field] = type_idx);
          }
          return _results;
        };

        GroupBy.prototype.to_java = function(parent_pipe) {
          var parent_jpipe, _ref;
          parent_jpipe = (_ref = this.parent_pipe) != null ? _ref.to_java() : void 0;
          return Cascading.Factory.GroupByBuffer(this.group_fields, this.sort_fields, this.incoming, this.incoming_types, this.outgoing, this.outgoing_types, Cascading.EnvironmentArgs, this.pipe_id, parent_jpipe);
        };

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

        EachStep.prototype.connect_to_incoming = function(source) {
          var add_field, add_type, field, k, type_idx, v, _i, _len, _ref, _ref2, _ref3, _results;
          if (source.is_each != null) {
            this.incoming = source.incoming;
            this.incoming_types = source.incoming_types;
          } else if (source.is_each_step != null) {
            this.incoming = source.outgoing;
            this.incoming_types = source.outgoing_types;
          } else {
            throw new Error("Cannot connect " + source + " to an each step");
          }
          this.outgoing = this.incoming.slice(0);
          this.outgoing_types = _.clone(this.incoming_types);
          _ref = this.spec;
          for (k in _ref) {
            v = _ref[k];
            if (k !== "add" && k !== "remove") {
              throw new Error("Invalid argument " + k + " for field spec");
            }
          }
          if (this.spec.add != null) {
            _ref2 = this.spec.add;
            for (add_field in _ref2) {
              add_type = _ref2[add_field];
              if (!_.include(this.outgoing, add_field)) {
                type_idx = U.type_idx_map[add_type];
                if (type_idx == null) {
                  throw new Error("Invalid type " + add_type + " for " + add_field);
                }
                this.outgoing[this.outgoing.length] = add_field;
                this.outgoing_types[add_field] = type_idx;
              }
            }
          }
          if (this.spec.remove != null) {
            _ref3 = this.spec.remove;
            _results = [];
            for (_i = 0, _len = _ref3.length; _i < _len; _i++) {
              field = _ref3[_i];
              if ((this.spec.add != null) && _.include(this.spec.add, field)) {
                throw new Error("Cannot remove field " + field + " being added in same step");
              }
              if (!_.include(this.incoming, field)) {
                throw new Error("Invalid field " + field + " being removed");
              }
              this.outgoing = _.without(this.outgoing, field);
              _results.push(delete this.outgoing_types[field]);
            }
            return _results;
          }
        };

        return EachStep;

      })()
    };
  });

}).call(this);
