define ->
  Cascade:
    class
      is_cascade: true

      constructor: ->
        @flows = []

      add_flow: (flow) ->
        throw new Error("Invalid flow") unless flow.is_flow?
        @flows.unshift(flow)
        flow

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

        @assemblies[assembly.name]
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
  Tap:
    class
      is_tap: true

      constructor: (@path, @type) ->

  Pipe:
    class Pipe
      is_pipe: true

      constructor: (@name, @parent_pipe) ->

  Each:
    class Each extends Pipe
      is_each: true

  Every:
    class Every extends Pipe
      is_every: true

  Assembly:
    class
      is_assembly: true

      constructor: (@name, parent) ->
        if parent.is_flow?
          @head_pipe = new Pipe(name)
        else
          @head_pipe = new Pipe(name, parent.tail_pipe)

        @tail_pipe = @head_pipe

      add_pipe: (pipe) ->
        pipe.parent_pipe = @tail_pipe
        @tail_pipe = pipe
