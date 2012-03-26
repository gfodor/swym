define ["./util"], (U) ->
  class Scheme
    scheme_type: ->
      @scheme_type

    is_scheme:
      true

  TextLine:
    class extends Scheme
      constructor: (@fields...) ->
        @scheme_type = "TextLine"
        @fields = ["line"] if @fields.length is 0
        @field_types = {}

        if @fields.length == 1
          @field_types[@fields[0]] = U.type_idx_map.string
        else if @fields.length == 2
          @field_types[@fields[0]] = U.type_idx_map.string
          @field_types[@fields[1]] = U.type_idx_map.string
        else
          throw new Error("TextLine scheme can have at most two fields.")

      to_java: ->
        Cascading.Factory.TextLine(@fields)
