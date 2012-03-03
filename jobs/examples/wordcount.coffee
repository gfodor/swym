require ["org/cascading/js/builder"], (builder) ->
  word_generator = (tuple, emitter) ->
    emitter({ word: word }) for words in tuple.line.match(/\S+/)
    null

  builder.cascade ($) ->
    $.flow 'word count', ->
      $.source 'input', $.tap("test.txt", scheme.TEXT)

      $.assembly 'input', ->
        $.each word_generator
        $.insert capitalized: (tuple) ->
          tuple.word.toUpperCase()

        $.group_by 'capitalized', ->
          $.count 'count'

      $.sink 'input', $.tap("count.txt", scheme.TEXT)