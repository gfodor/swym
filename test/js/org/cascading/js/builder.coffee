modules = ["builder", "schemes"]

paths = ("../../../../../src/js/org/cascading/js/#{module}" for module in modules)

require paths, (builder, schemes) ->
  describe "job builder", ->
    it "should work", ->
      builder.cascade ($) ->
        $.flow 'word_counter', ->
          $.source 'input', $.tap("test.txt", new schemes.TextLine())
          console.log "in flow"

