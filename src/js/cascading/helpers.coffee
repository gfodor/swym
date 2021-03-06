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
      assembly = this.register(@flow.add_assembly(new components.Assembly(name, @flow)))
      @assembly_stack.push(assembly)
      f(this)
      @assembly_stack.pop()
      assembly

    current_assembly: ->
      @assembly_stack[0]

    group_by: (fields, params, f) ->
      throw new Error("Cannot nest group bys") if this.is_in_group_by()

      f = params if typeof(params) == "function" and not f?

      if typeof(fields) == "string"
        fields = [fields]

      @current_group_by = new components.GroupBy(this.current_assembly(), fields, params)
      f()
      @current_group_by = null

    foreach_group: (fields, params, initializer, processor, finalizer) ->
      if typeof(fields) == "string"
        fields = [fields]

      new components.GroupBy(this.current_assembly(), fields, params, initializer, processor, finalizer)

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

    map: (spec, callback) ->
      each = this.current_assembly().current_each()
      new components.EachStep(each, spec, callback)

  EveryPipes:
    count: ->

  Schemes:
    text_line_scheme: (fields...) ->
      new schemes.TextLine(fields...)

