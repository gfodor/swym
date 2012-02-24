modules = ["builder", "schemes"]

paths = ("../../../../../src/js/org/cascading/js/#{module}" for module in modules)

require paths, (builder, schemes) ->
  describe "job builder", ->
    it "should work", ->
      c = builder.cascade ($) ->
        $.flow 'word_counter', ->
          $.source 'input', $.tap("test.txt", new schemes.TextLine())

          $.assembly 'input', ->
            $.generator ["line"], ["word"], (tuple, emitter) ->
              emitter({ word: word }) for word in tuple.line.match(/\S+/g)

            $.insert capitalized: (tuple) ->
              tuple.word.toUpperCase()

            #$.group_by 'capitalized', ->

          $.sink 'input', $.tap("output", new schemes.TextLine())

      c.to_java()

