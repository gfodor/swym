define ->
  class Scheme
    scheme_type: ->
      @scheme_type

    is_scheme:
      true

  TextLine:
    class extends Scheme
      constructor: (@fields...) ->
        @scheme_type = "TextLine"
        @fields = ["line"] if @fields.length == 0

      to_java: ->
        Cascading.Factory.TextLine(@fields)
