define ["./components", "./schemes"], (components, schemes) ->
  class Builder
    constructor: ->
      @stack = []
      @components = { }
      @sources = { }

    add: (name, component) ->
      throw new Error("Duplicate component #{name}") if @components[name]
      @stack.push(component)
      @components[name] = component

    get: (name) ->
      @components[name]

    cascade: (f) ->
      @cascade = new components.Cascade()
      f(this)

    flow: (name, f) ->
      throw new Error "No cascade created" unless @cascade
      flow = this.add(@cascade.add(new components.Flow(name)))
      f(this)

    source: (name, tap) ->
      throw new Error "Invalid tap specified" unless tap.is_tap?
      @sources[name] = tap

    tap: (path, scheme) ->
      unless scheme.is_scheme?
        throw new Error "Invalid scheme #{scheme.constructor} vs #{schemes.TextLine}"

      new components.Tap(path, scheme)

  cascade: (f) ->
    builder = new Builder()
    builder.cascade(f)
