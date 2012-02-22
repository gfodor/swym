define ["./components", "./schemes", "./helpers", "./common"], (components, schemes, helpers, common) ->
  class Builder extends common.Module
    @include helpers.Components
    @include helpers.EachPipes
    @include helpers.EveryPipes

    constructor: ->
      @components = { }
      @assembly_stack = []

    register: (component) ->
      throw new Error("Duplicate component #{component.name}") if @components[component.name]?
      @components[component.name] = component

    get: (name) ->
      @components[name]

  cascade: (f) ->
    builder = new Builder()
    builder.cascade(f)
