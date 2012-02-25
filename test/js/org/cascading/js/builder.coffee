for key, value of require_commonjs("jasmine")
  this[key] = value

modules = ["builder", "schemes"]

paths = ("../../../../../src/js/org/cascading/js/#{module}" for module in modules)

require paths, (builder, schemes) ->
  describe "job builder", ->
    it "should work", ->
      c = builder.cascade ($) ->
        $.flow 'word_counter', ->
          $.source 'input', $.tap("listings.txt", new schemes.TextLine())

          $.assembly 'input', ->
            $.generator ["line"], ["word"], (tuple, emitter) ->
              words = tuple.line.match(/\S+/g)

              for word in words
                emitter(word)

          $.sink 'input', $.tap("output", new schemes.TextLine())
#
#      c.to_java()
#
