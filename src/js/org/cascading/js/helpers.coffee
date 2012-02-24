define ["./components", "./schemes"], (components, schemes) ->
  Components:
    cascade: (f) ->
      @cascade = new components.Cascade()
      f(this)
      @cascade

    flow: (name, f) ->
      throw new Error "No cascade created" unless @cascade
      @flow = this.register(@cascade.add_flow(new components.Flow(name)))
      f(this)

    source: (name, tap) ->
      throw new Error "No flow created" unless @flow
      throw new Error "Cannot define source inside assembly" if this.current_assembly()?

      @flow.add_source(name, tap)

    sink: (name, tap) ->
      throw new Error "No flow created" unless @flow
      throw new Error "Cannot define sink inside assembly" if this.current_assembly()?

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
      @assembly_stack[0]

    group_by: (fields, params, f) ->
      throw new Error("Cannot nest group bys") if this.is_in_group_by()

      f = params if typeof(params) == "function" && !f?

      if typeof(fields) == "string"
        fields = [fields]

      @current_group_by = new components.GroupBy(fields, params)
      this.current_assembly().add_pipe(@current_group_by)
      f()
      @current_group_by = null

    is_in_group_by: ->
      @current_group_by?

  EachPipes:
    insert: (params) ->
      for name, value of params
        (=>
          [n, v] = [name, value]

          if typeof(v) == 'function'
            this.generator [], [name], (tuple, emitter) ->
              out = {}
              out[n] = v(tuple)
              emitter(out)
          else
            this.generator [], [name], (tuple, emitter) ->
              out = {}
              out[n] = v
              emitter(out))()

    generator: (argument_selector, result_fields, callback) ->
      throw new Error("Cannot define map pipe inside of group by") if this.is_in_group_by()
      pipe = new components.Each(components.EachTypes.GENERATOR, callback, argument_selector, result_fields)
      this.current_assembly().add_pipe(pipe)
      pipe

    filter: (callback) ->
      throw new Error("Cannot define filter pipe inside of group by") if this.is_in_group_by()
      pipe = new components.Each(components.EachTypes.FILTER, callback)
      this.current_assembly().add_pipe(pipe)
      pipe


  EveryPipes:
    count: ->

