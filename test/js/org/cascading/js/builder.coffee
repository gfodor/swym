modules = ["builder", "schemes"]

paths = ("../../../../../src/js/org/cascading/js/#{module}" for module in modules)

require paths, (builder, schemes) ->
  describe "job builder", ->
    it "should work", ->
      c = builder.cascade ($) ->
        $.flow 'word_counter', ->
          $.source 'input', $.tap("listings.txt", new schemes.TextLine())

          $.assembly 'input', ->
            getFunction = null

            $.generator ["line"], ["word"], (tuple, emitter) ->
              emitter({ word: word }) for word in tuple.line.match(/\S+/g)

            $.generator ["word"], ["word2"], (tuple, emitter) ->
              emitter({ word2: word }) for word in tuple.word.match(/\S+/g)

            #$.generator ["word2"], ["word3"], (tuple, emitter) ->
            #  emitter({ word3: word }) for word in tuple.word2.match(/\S+/g)

            #$.generator ["word3"], ["word4"], (tuple, emitter) ->
            #  emitter({ word4: word }) for word in tuple.word3.match(/\S+/g)

            #$.generator ["word4"], ["word5"], (tuple, emitter) ->
            #  emitter({ word5: word }) for word in tuple.word4.match(/\S+/g)

            #$.generator ["word5"], ["word6"], (tuple, emitter) ->
            #  emitter({ word6: word }) for word in tuple.word5.match(/\S+/g)

            #$.generator ["word6"], ["word7"], (tuple, emitter) ->
            #  emitter({ word7: word }) for word in tuple.word6.match(/\S+/g)

            #$.generator ["word7"], ["word8"], (tuple, emitter) ->
            #  emitter({ word8: word }) for word in tuple.word7.match(/\S+/g)

            #$.generator ["word8"], ["word9"], (tuple, emitter) ->
            #  emitter({ word9: word }) for word in tuple.word8.match(/\S+/g)

            #$.insert capitalized: (tuple) ->
            #  tuple.word.toUpperCase()

            #$.group_by 'capitalized', ->

          $.sink 'input', $.tap("output", new schemes.TextLine())

      c.to_java()

