job ($, _) ->
  $.flow 'word count', ->
    $.source 'input', $.tap("words3.txt", $.text_line_scheme("offset", "word"))

    assembly = $.assembly 'input', ->
      #$.map { add: ["word", "word2"], remove: ["line", "offset"] }, (tuple, writer) ->
      #  for word in tuple.line.match(/\S+/g)
      #    writer({ word: word })

      last_key = null
      count = 0

      $.foreach_group ["word"], { add: ["count"], types: { count: "int" } },
        ((tuple) -> count = 0),
        ((tuple, writer) ->
          count += 1),
        ((tuple, writer) ->
          writer({count: count})
          )

        #$.aggregate "count", (keys, values, context) ->
        #  if context is null
        #    context = 0

        #  context += 1

        #$.aggregate "average_word2_length",
        #  ((keys, values, context) ->
        #    context = [] if context is null
        #    context[0] += values.word2.length
        #    context[1] += 1
        #    context),
        #  ((context) ->
        #    context[0] / context[1])


    $.sink 'input', $.tap("output", $.text_line_scheme("word", "count"))
