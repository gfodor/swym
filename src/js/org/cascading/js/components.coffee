define ->
  Cascade:
    class
      is_cascade: true

      constructor: ->
        @flows = []

      add: (flow) ->
        @flows.unshift(flow)
        flow

  Flow:
    class
      is_flow: true

      constructor: (@name) ->

  Tap:
    class
      is_tap: true

      constructor: (@path, @type) ->

