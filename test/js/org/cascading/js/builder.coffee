modules = ["builder", "schemes"]

paths = ("../../../../../../build/src/js/org/cascading/js/#{module}" for module in modules)

#insert - declare new columns
#replace - declare replacement map
#remove - declare removal list
# pipe argument selector is whole set

listing_fields = [ "listing_id", "state", "user_id", "title", "description", "creation_tsz",
  "ending_tsz", "original_creation_tsz", "last_modified_tsz", "price", "currency_code", "quantity",
  "tags", "materials", "section_id", "featured_rank", "views", "image_listing_id",
  "state_tsz", "last_modified_tsz_epoch", "saturation", "brightness", "is_black_and_white"
]

require paths, (builder, schemes) ->
  with_test_flow = (f) ->
    cascade = builder.cascade ($) ->
      $.flow 'word_counter', ->
        f($)

  describe "job builder", ->
    it "should set outgoing scope on tap", ->
      with_test_flow ($) ->
        tap = $.source 'input', $.tap("listings.txt", new schemes.TextLine())
        expect(tap.outgoing).toEqual ["line"]

    it "should set incoming scope on assembly", ->
      with_test_flow ($) ->
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1"))
        $.source 'input_2', $.tap("listings.txt", new schemes.TextLine("line_2"))
        console.log($.assembly)

        a1 = $.assembly 'input', ->
        a2 = $.assembly 'input_2', ->

        expect(a1.incoming).toEqual ["offset", "line_1"]
        expect(a2.incoming).toEqual ["line_2"]



      #c = builder.cascade ($) ->
      #  $.flow 'word_counter', ->
      #    $.source 'input', $.tap("listings.txt", new schemes.TextLine())

      #    $.assembly 'input', ->
      #      getFunction = null

      #      $.generator ["line"], ["word"], (tuple, emitter) ->
      #        emitter({ word: word }) for word in tuple.line.match(/\S+/g)

      #      $.generator ["word"], ["word2"], (tuple, emitter) ->
      #        emitter({ word2: word }) for word in tuple.word.match(/\S+/g)

      #      #$.generator ["word2"], ["word3"], (tuple, emitter) ->
      #      #  emitter({ word3: word }) for word in tuple.word2.match(/\S+/g)

      #      #$.generator ["word3"], ["word4"], (tuple, emitter) ->
      #      #  emitter({ word4: word }) for word in tuple.word3.match(/\S+/g)

      #      #$.generator ["word4"], ["word5"], (tuple, emitter) ->
      #      #  emitter({ word5: word }) for word in tuple.word4.match(/\S+/g)

      #      #$.generator ["word5"], ["word6"], (tuple, emitter) ->
      #      #  emitter({ word6: word }) for word in tuple.word5.match(/\S+/g)

      #      #$.generator ["word6"], ["word7"], (tuple, emitter) ->
      #      #  emitter({ word7: word }) for word in tuple.word6.match(/\S+/g)

      #      #$.generator ["word7"], ["word8"], (tuple, emitter) ->
      #      #  emitter({ word8: word }) for word in tuple.word7.match(/\S+/g)

      #      #$.generator ["word8"], ["word9"], (tuple, emitter) ->
      #      #  emitter({ word9: word }) for word in tuple.word8.match(/\S+/g)

      #      #$.insert capitalized: (tuple) ->
      #      #  tuple.word.toUpperCase()

      #      #$.group_by 'capitalized', ->

      #    $.sink 'input', $.tap("output", new schemes.TextLine())

      #c.to_java()

