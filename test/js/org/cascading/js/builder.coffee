modules = ["builder", "schemes"]

paths = ("../../../../../src/js/org/cascading/js/#{module}" for module in modules)

require paths, (builder, schemes) ->
  describe "job builder", ->
    it "should work", ->
      builder.cascade ($) ->
        $.flow 'word_counter', ->
          $.source 'input', $.tap("test.txt", new schemes.TextLine())

          $.assembly 'input', ->
            $.map (tuple, emitter) ->
              emitter({ word: word }) for words in tuple.line.match(/\S+/)

            $.insert capitalized: (tuple) ->
              tuple.word.toUpperCase()

            $.group_by 'capitalized', ->

          $.sink 'input', $.tap("count.txt", new schemes.TextLine())

