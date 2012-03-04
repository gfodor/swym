#require( { baseUrl: '../../lib/js' } )

job ($) ->
  $.flow 'word count', ->
    $.source 'input', $.tap("data/listings.txt", $.text_line_scheme("offset", "line"))

    assembly = $.assembly 'input', ->
      $.map { add: ["word"], remove: ["line", "offset"] }, (tuple, writer) ->
        for word in tuple.line.match(/\S+/g)
          writer({ word: word })

      $.map { }, (tuple, writer) -> tuple.word = tuple.word.toUpperCase() ; writer(tuple)
      $.map { add: ["foo"] }, (tuple, writer) ->
        tuple.foo = "hi"
        writer(tuple)

    $.sink 'input', $.tap("output", $.text_line_scheme("word", "foo"))
