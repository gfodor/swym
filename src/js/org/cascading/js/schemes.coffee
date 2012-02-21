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
