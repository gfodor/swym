define ["./components", "./schemes"], (components, schemes) ->
  Components:
    cascade: (f) ->
      @cascade = new components.Cascade()
      f(this)

    flow: (name, f) ->
      throw new Error "No cascade created" unless @cascade
      @flow = this.register(@cascade.add_flow(new components.Flow(name)))
      f(this)

    source: (name, tap) ->
      throw new Error "No flow created" unless @flow
      @flow.add_source(name, tap)

    sink: (name, tap) ->
      throw new Error "No flow created" unless @flow
      @flow.add_sink(name, tap)

    tap: (path, scheme) ->
      unless scheme.is_scheme?
        throw new Error "Invalid scheme #{scheme.constructor} vs #{schemes.TextLine}"

      new components.Tap(path, scheme)

    assembly: (name, f) ->
      throw new Error "No flow created" unless @flow
      @assembly = this.register(@flow.add_assembly(new components.Assembly(name, @flow)))
      @assembly_stack.push(@assembly)
      f(this)
      @assembly_stack.pop()

    current_assembly: ->
      throw new Error("No assembly in scope") unless stack[0]?.is_assembly?
      @assembly_stack[0]

  EachPipes:
    insert: ->
    each: (f) ->

  EveryPipes:
    count: ->

