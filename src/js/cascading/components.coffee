define ->
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
        if parent.is_flow?
          @head_pipe = new Pipe(name)
          @flow = parent
        else
          @head_pipe = new Pipe(name, parent.tail_pipe)
          @flow = parent.flow

        @tail_pipe = @head_pipe

        source = @flow.sources[@name]

        unless source
          throw new Error("Unknown source #{@name} for assembly")

        @head_pipe.incoming = @head_pipe.outgoing = source.outgoing

      add_pipe: (pipe) ->
        pipe.parent_pipe = @tail_pipe
        @tail_pipe = pipe

      to_java: ->
        @tail_pipe.to_java(@tail_pipe.parent_pipe)
  Pipe:
    class Pipe
      @registerPipeCallback: (callback, pipe_index, callback_type) =>
        callback_type ?= "default"
        this.pipeCallbacks ?= {}
        this.pipeCallbacks[pipe_index] ?= {}
        this.pipeCallbacks[pipe_index][callback_type] = callback

      @invokePipeCallback: =>
        pipe_index = arguments[0]
        callback_type = arguments[1]
        tupleBuffer = arguments[2]
        operation = arguments[3]
        call = arguments[4]

        callback_type ?= "default"
        callback = this.pipeCallbacks[pipe_index][callback_type]

        callback.apply(this, [tupleBuffer, operation, call])

      @getNextPipeIndex: =>
        this.current_pipe_index ?= 0
        this.current_pipe_index += 1

      is_pipe: true

      constructor: (@name, @parent_pipe) ->
        @parent_pipe ?= null
        @pipe_index = Pipe.getNextPipeIndex()

      to_java: ->
        parent_jpipe = @parent_pipe?.to_java()

        if parent_jpipe?
          Cascading.Factory.Pipe(@name, parent_jpipe)
        else
          Cascading.Factory.Pipe(@name)

  Each:
    class Each extends Pipe
      is_each: true

      constructor: (@type, outer_callback, @argument_selector, @result_fields) ->
        super

        buf = new Array(8 * 1024 + 128)
        c_buf = 0
        flushFromV8 = null
        getMethod = null
        tuple = {}

        @callback = (tupleBuffer, operation, call) =>
          flushFromV8 ?= operation.flushFromV8

          for i_tuple in [0...tupleBuffer.length / @argument_selector.length]
            for i_field in [0...@argument_selector.length]
              idx = (i_tuple * @argument_selector.length) + i_field
              tuple[@argument_selector[i_field]] = tupleBuffer[idx]

            outer_callback(tuple, (t) =>
              for field in @result_fields
                v = t[field]

                if v?
                  buf[c_buf] = v
                else
                  buf[c_buf] = null

                c_buf += 1

              if c_buf >= 8 * 1024
                flushFromV8.apply(operation, [buf, c_buf, call])
                c_buf = 0)

          flushFromV8.apply(operation, [buf, c_buf, call])

        Pipe.registerPipeCallback(@callback, @pipe_index)

      to_java: (parent_pipe) ->
        if @type == EachTypes.GENERATOR
          parent_jpipe = @parent_pipe?.to_java()
          Cascading.Factory.GeneratorEach(@argument_selector, @result_fields, Cascading.EnvironmentArgs, @pipe_index, parent_jpipe)

  Every:
    class Every extends Pipe
      is_every: true

  GroupBy:
    class GroupBy extends Pipe
      is_group_by: true

      constructor: (@group_fields, params) ->
        throw new Error("Invalid group by fields #{@group_fields}") unless typeof(@group_fields) == "object"
        @sort_fields = params?.sort_fields ? []

  CoGroup:
    class CoGroup extends Pipe
      is_co_group: true

