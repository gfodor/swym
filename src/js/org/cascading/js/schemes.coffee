env = require_commonjs("cascading/env") if require_commonjs?

define ->
  class Scheme
    scheme_type: ->
      @scheme_type

    is_scheme:
      true

  TextLine:
    class extends Scheme
      constructor: (@fields) ->
        @scheme_type = "TextLine"
        @fields ?= ["line"]

      to_java: ->
        env.getFactory().TextLine(@fields)
