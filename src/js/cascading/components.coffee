define ["underscore", "./util"], (_, U) ->
  EachTypes =
    GENERATOR: 0
    FILTER: 1

  EachTypes: EachTypes

  Cascade:
    class
      is_cascade: true

      constructor: ->
        @flows = []

      add_flow: (flow) ->
        throw new Error("Invalid flow") unless flow.is_flow?
        @flows.unshift(flow)
        flow

      to_java: ->
        jflows = for flow in @flows
          flow.to_java()

  Flow:
    class
      is_flow: true

      constructor: (@name) ->
        @assemblies = {}
        @sources = {}
        @sinks = {}

      add_assembly: (assembly) ->
        throw new Error("Duplicate assembly #{assembly.name}") if @assemblies[assembly.name]
        throw new Error("Not an assembly") unless assembly.is_assembly?

        @assemblies[assembly.name] = assembly
        assembly

      add_source: (name, tap) ->
        throw new Error("Duplicate source #{name}") if @sources[name]
        throw new Error("Not a tap") unless tap.is_tap?

        @sources[name] = tap
        tap

      add_sink: (name, tap) ->
        throw new Error("Duplicate sink #{name}") if @sinks[name]
        throw new Error("Not a tap") unless tap.is_tap?

        @sinks[name] = tap
        tap

      to_java: ->
        jsources = {}
        jsinks = {}

        for name, tap of @sources
          jsources[name] = tap.to_java()

        for name, tap of @sinks
          jsinks[name] = tap.to_java()

        jtail_pipes = []

        for name, assembly of @assemblies
          jtail_pipes.unshift(assembly.to_java())

        Cascading.Factory.Flow(@name, jsources, jsinks, jtail_pipes)

  Tap:
    class
      is_tap: true

      constructor: (@path, @scheme) ->
        @outgoing = @scheme.fields
        @outgoing_types = @scheme.field_types
        @is_bound = false

      to_java: ->
        jscheme = @scheme.to_java()
        Cascading.Factory.Hfs(jscheme, @path)

  Assembly:
    class
      is_assembly: true

      constructor: (@name, parent) ->
        @flow = if parent.is_flow? then parent else parent.flow
        @source = @flow.sources[@name]

        unless @source
          throw new Error("Unknown source #{@name} for assembly")

        if parent.is_flow?
          new Pipe(this, name)
        else
          new Pipe(this, name, parent.tail_pipe)

      current_each: ->
        @last_each_pipe ?= new Each(this)

      add_pipe: (pipe) ->
        @last_each_pipe = pipe if pipe.is_each?

        unless @tail_pipe?
          @head_pipe = pipe
          pipe.connect_to_incoming(@source)
        else
          pipe.parent_pipe = @tail_pipe
          pipe.connect_to_incoming(@tail_pipe)

        @tail_pipe = pipe

      to_java: ->
        @tail_pipe.to_java(@tail_pipe.parent_pipe)

  Pipe:
    class Pipe
      @register_pipe: (pipe) =>
        @current_pipe_id ?= 0
        @current_pipe_id += 1

        pipe.pipe_id = this.current_pipe_id

        @pipes ?= {}
        @pipes[pipe.pipe_id] = pipe

      @set_pipe_out_buffers: (out_buffers, pipe_id) =>
        pipe = @pipes[pipe_id]
        pipe.out_buffers = out_buffers
        num_types = _.keys(U.type_idx_map).length
        out_field_types = out_buffers[num_types]
        out_field_data_offsets = out_buffers[num_types + 1]
        out_num_fields_per_type = out_buffers[num_types + 2]

        current_offsets = []
        pipe.out_obj = {}

        for type, idx of U.type_idx_map
          out_num_fields_per_type[idx] = 0
          current_offsets[current_offsets.length] = 0

        writable_fields = pipe.outgoing
        writable_fields = _.difference(writable_fields, pipe.group_fields) if pipe.group_fields

        for i_field, field of writable_fields
          type_idx = pipe.outgoing_types[field]
          out_num_fields_per_type[type_idx] += 1
          offset = current_offsets[type_idx]
          out_field_data_offsets[i_field] = offset
          out_field_types[i_field] = type_idx

          do (type_idx, offset) ->
            pipe.out_obj[field] = (val) ->
              pipe.out_buffers[type_idx][offset] = val

          current_offsets[type_idx] += 1

      @get_flush_routine: (in_buffer, out_buffer, pipe_id) =>
        pipe = @pipes[pipe_id]

        initializer = pipe.initializer ? ->
        processor = pipe.processor ? ->
        finalizer = pipe.finalizer ? ->

        initialized = false

        group_fields = pipe.group_fields ? []
        non_group_in_fields = _.difference(pipe.incoming, group_fields)
        non_group_out_fields = _.difference(pipe.outgoing, group_fields)
        num_group_fields = group_fields.length
        stub_fields = _.map [0...num_group_fields], (i) -> "___swym_stub_gk_#{i}"

        current_group = {}
        tuple = {}

        writer = (tuple) ->
          for i_group_field in [0...num_group_fields]
            out_buffer[stub_fields[i_group_field]](current_group[group_fields[i_group_field]])

          for field in non_group_out_fields
            out_buffer[field](tuple[field])

          out_buffer.next_result()

        (last_group_is_complete, input_is_complete) ->
          while true
            for group_field in group_fields
              val = in_buffer[group_field]()
              tuple[group_field] = current_group[group_field] = val

            while true
              for field in non_group_in_fields
                tuple[field] = in_buffer[field]()

              unless initialized
                initializer(tuple)
                initialized = true

              processor(tuple, writer)

              break unless in_buffer.next_arg()

            has_another_group = in_buffer.next_group()

            if has_another_group || last_group_is_complete
              finalizer(tuple, writer)
              initializer(tuple)

            break unless has_another_group

          out_buffer.flush() if input_is_complete

      is_pipe: true

      constructor: (assembly, @name, parent_pipe) ->
        @parent_pipe ?= parent_pipe
        Pipe.register_pipe(this)
        assembly.add_pipe(this)

      connect_to_incoming: (source) ->
        @incoming = @outgoing = source.outgoing
        @incoming_types = @outgoing_types = source.outgoing_types

      to_java: ->
        parent_jpipe = @parent_pipe?.to_java()

        if parent_jpipe?
          out = Cascading.Factory.Pipe(@name, parent_jpipe)
        else
          out = Cascading.Factory.Pipe(@name)

  Each:
    class Each extends Pipe
      is_each: true

      constructor: (assembly) ->
        super(assembly)

        @steps = []

      add_step: (step) ->
        throw new Error("Not an each step") unless step.is_each_step?

        tail_step = _.last(@steps)
        step.connect_to_incoming(if tail_step then tail_step else this)

        @steps[@steps.length] = step

        @outgoing = step.outgoing
        @outgoing_types = step.outgoing_types

        # Re-generate the processor each time a new step is added
        @processor = @build_processor()

        step

      # Builds a processor function that expects a tuple and a function called 
      # the receiver.

      # The processor, when passed a tuple, will run it through via the 
      # user specified step functions on each step of this pipe and 
      # send the result to the specified reciever function.
      build_processor: ->
        functions = _.pluck @steps, "function"
        writers = []

        # generates the 'writer' functions passed to each user-specified
        # function for the steps. These writer functions just pass the received
        # tuple along to the next step's function, or to the final receiver.
        get_writer = (level, receiver) ->
          current_writer = writers[level]
          return current_writer if current_writer
          return receiver if functions.length == 0

          f = functions[level + 1]

          if f
            next_writer = get_writer(level + 1, receiver)

            writers[level] = (tuple) ->
              f tuple, next_writer
          else
            writers[level] = (tuple) ->
              receiver tuple

        (tuple, receiver) ->
          functions[0] tuple, get_writer(0, receiver)

      to_java: (parent_pipe) ->
        parent_jpipe = @parent_pipe?.to_java()
        Cascading.Factory.Each(@incoming, @incoming_types, @outgoing, @outgoing_types,
                               Cascading.EnvironmentArgs, @pipe_id, parent_jpipe)

  Every:
    class Every extends Pipe
      is_every: true

  GroupBy:
    class GroupBy extends Pipe
      is_group_by: true

      constructor: (assembly, @group_fields, @spec, @initializer, @processor, @finalizer) ->
        super(assembly)
        throw new Error("Invalid group by fields #{@group_fields}") unless typeof(@group_fields) == "object"
        @sort_fields = @spec?.sort_fields ? []

      connect_to_incoming: (source) ->
        @incoming = source.outgoing
        @incoming_types = source.outgoing_types

        @outgoing = @group_fields.slice(0)
        @outgoing_types = {}

        for group_field in @group_fields
          unless _.include(@incoming, group_field)
            throw new Error("No such field #{group_field} in incoming")

          unless @incoming_types[group_field]?
            throw new Error("No such field #{group_field} in incoming types")

          @outgoing_types[group_field] = @incoming_types[group_field]

        for k, v of @spec
          if k isnt "add" and k isnt "sort" and k isnt "types"
            throw new Error("Invalid argument #{k} for group by spec")

        for add_field, add_type of @spec.add
          type_idx = U.type_idx_map[add_type]
          throw new Error("Invalid type #{add_type} for #{add_field}") unless type_idx?

          @outgoing[@outgoing.length] = add_field
          @outgoing_types[add_field] = type_idx

      to_java: (parent_pipe) ->
        parent_jpipe = @parent_pipe?.to_java()

        Cascading.Factory.GroupByBuffer(@group_fields, @sort_fields,
                                        @incoming, @incoming_types, @outgoing, @outgoing_types,
                                        Cascading.EnvironmentArgs, @pipe_id, parent_jpipe)

  CoGroup:
    class CoGroup extends Pipe
      is_co_group: true

  EachStep:
    class EachStep
      is_each_step: true

      constructor: (@each, @spec, @function) ->
        @each.add_step(this)

      connect_to_incoming: (source) ->
        if source.is_each?
          @incoming = source.incoming
          @incoming_types = source.incoming_types
        else if source.is_each_step?
          @incoming = source.outgoing
          @incoming_types = source.outgoing_types
        else
          throw new Error("Cannot connect " + source + " to an each step")

        @outgoing = @incoming.slice(0)
        @outgoing_types = _.clone(@incoming_types)

        for k, v of @spec
          if k isnt "add" and k isnt "remove"
            throw new Error("Invalid argument #{k} for field spec")

        if @spec.add?
          for add_field, add_type of @spec.add
            unless _.include(@outgoing, add_field)
              type_idx = U.type_idx_map[add_type]
              throw new Error("Invalid type #{add_type} for #{add_field}") unless type_idx?

              @outgoing[@outgoing.length] = add_field
              @outgoing_types[add_field] = type_idx

        if @spec.remove?
          for field in @spec.remove
            throw new Error("Cannot remove field #{field} being added in same step") if @spec.add? and _.include @spec.add, field
            throw new Error("Invalid field #{field} being removed") if not _.include(@incoming, field)

            @outgoing = _.without @outgoing, field
            delete @outgoing_types[field]
