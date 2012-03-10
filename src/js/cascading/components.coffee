define ["underscore"], (_) ->
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
          pipe.connect_to_incoming(@source.outgoing)
        else
          pipe.parent_pipe = @tail_pipe
          pipe.connect_to_incoming(@tail_pipe.outgoing)

        @tail_pipe = pipe

      to_java: ->
        @tail_pipe.to_java(@tail_pipe.parent_pipe)

  Pipe:
    class Pipe
      @register_pipe: (pipe) =>
        this.current_pipe_id ?= 0
        this.current_pipe_id += 1

        pipe.pipe_id = this.current_pipe_id

        this.pipes ?= {}
        this.pipes[pipe.pipe_id] = pipe

      @set_pipe_out_buffer: (out_buffer, pipe_id) ->
        pipe = Pipe.pipes[pipe_id].out_buffer = out_buffer

      @get_group_start_processor: (group_tuple, argument_tuple, pipe_id) ->
        self = this
        pipe = Pipe.pipes[pipe_id]

        ->
          pipe.initializer(group_tuple, argument_tuple, null)
          console.log("Got new group " + group_tuple.word())

      @get_argument_processor: (group_tuple, argument_tuple, pipe_id) ->
        self = this
        pipe = Pipe.pipes[pipe_id]

        ->
          pipe.processor(group_tuple, argument_tuple, null)
          console.log("Got new argument " + group_tuple.word())

      @get_group_end_processor: (group_tuple, argument_tuple) ->
        self = this
        pipe = Pipe.pipes[pipe_id]

        ->
          pipe.finalizer(group_tuple, argument_tuple, null)
          console.log("Finish group " + group_tuple.word())

      is_pipe: true

      constructor: (assembly, @name, parent_pipe) ->
        @parent_pipe ?= parent_pipe
        Pipe.register_pipe(this)
        assembly.add_pipe(this)

      connect_to_incoming: (incoming) ->
        @incoming = @outgoing = incoming

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
        step.connect_to_incoming(if tail_step then tail_step.outgoing else @incoming)

        @steps[@steps.length] = step
        @outgoing = step.outgoing

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
        Cascading.Factory.Each(@incoming, @outgoing, Cascading.EnvironmentArgs, @pipe_id, parent_jpipe)

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

      connect_to_incoming: (incoming) ->
        @incoming = incoming
        @outgoing = @group_fields.slice(0)

        for group_field in @group_fields
          throw new Error("No such field #{group_field} in incoming") unless _.include(@incoming, group_field)

        for k, v of @spec
          if k isnt "add" and k isnt "sort"
            throw new Error("Invalid argument #{k} for group by spec")

        for add_field in @spec.add
          @outgoing[@outgoing.length] = add_field

        @outgoing_types = {}

        for outgoing_field in @outgoing
          @outgoing_types[outgoing_field] = -1 # Type.UNKNOWN

      to_java: (parent_pipe) ->
        parent_jpipe = @parent_pipe?.to_java()
        Cascading.Factory.GroupByBuffer(@group_fields, @sort_fields, @incoming, @outgoing,
                                        Cascading.EnvironmentArgs, @pipe_id, parent_jpipe)

  CoGroup:
    class CoGroup extends Pipe
      is_co_group: true

  EachStep:
    class EachStep
      is_each_step: true

      constructor: (@each, @spec, @function) ->
        @each.add_step(this)

      connect_to_incoming: (incoming) ->
        @incoming = incoming
        @outgoing = @incoming.slice(0)

        for k, v of @spec
          if k isnt "add" and k isnt "remove"
            throw new Error("Invalid argument #{k} for field spec")

        if @spec.add?
          for field in @spec.add
            @outgoing[@outgoing.length] = field unless _.include(@outgoing, field)

        if @spec.remove?
          for field in @spec.remove
            throw new Error("Cannot remove field #{field} being added in same step") if @spec.add? and _.include @spec.add, field
            throw new Error("Invalid field #{field} being removed") if not _.include(@incoming, field)

            @outgoing = _.without @outgoing, field
