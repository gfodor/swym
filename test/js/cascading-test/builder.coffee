#insert - declare new columns
#replace - declare replacement map
#remove - declare removal list
# pipe argument selector is whole set

listing_fields = [ "listing_id", "state", "user_id", "title", "description", "creation_tsz",
  "ending_tsz", "original_creation_tsz", "last_modified_tsz", "price", "currency_code", "quantity",
  "tags", "materials", "section_id", "featured_rank", "views", "image_listing_id",
  "state_tsz", "last_modified_tsz_epoch", "saturation", "brightness", "is_black_and_white"
]

require { baseUrl: "lib/js" }, ["cascading/builder", "cascading/schemes", "underscore"], (builder, schemes, _) ->
  with_test_flow = (f) ->
    cascade = builder.cascade ($) ->
      $.flow 'word_counter', ->
        f($)

  expect_bad_flow = (msg, f) ->
    expect(-> with_test_flow(f)).toThrow(new Error(msg))

  describe "job builder", ->
    it "should set outgoing scope on tap", ->
      with_test_flow ($) ->
        tap = $.source 'input', $.tap("listings.txt", new schemes.TextLine())
        expect(tap.outgoing).toEqual ["line"]

      with_test_flow ($) ->
        tap = $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line"))
        expect(tap.outgoing).toEqual ["offset", "line"]

    it "should fail if trying to tap bad source name", ->
      expect_bad_flow "Unknown source bad_input for assembly", ($) ->
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1"))

        a1 = $.assembly 'bad_input', ->

        $.sink 'input', $.tap("output", new schemes.TextLine())

    it "should set incoming scope on assembly", ->
      with_test_flow ($) ->
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1"))
        $.source 'input_2', $.tap("listings.txt", new schemes.TextLine("line_2"))

        a1 = $.assembly 'input', ->
        a2 = $.assembly 'input_2', ->

        expect(a1.head_pipe.incoming).toEqual ["offset", "line_1"]
        expect(a1.head_pipe.outgoing).toEqual ["offset", "line_1"]
        expect(a2.head_pipe.incoming).toEqual ["line_2"]
        expect(a2.head_pipe.incoming).toEqual ["line_2"]

    it "should fail if no sinks", ->
    it "should fail if no assembly for sink", ->
    it "should fail if duplicate assembly", ->
      expect_bad_flow "Duplicate assembly input", ($) ->
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1"))

        a1 = $.assembly 'input', ->
        a1 = $.assembly 'input', ->

        $.sink 'input', $.tap("output", new schemes.TextLine())

    it "should fail if duplicate source", ->
      expect_bad_flow "Duplicate source input", ($) ->
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1"))
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1"))

        a1 = $.assembly 'input', ->

        $.sink 'input', $.tap("output", new schemes.TextLine())

    it "should fail if duplicate sink", ->
      expect_bad_flow "Duplicate sink input", ($) ->
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1"))

        a1 = $.assembly 'input', ->

        $.sink 'input', $.tap("output", new schemes.TextLine())
        $.sink 'input', $.tap("output", new schemes.TextLine())

    it "should fail if any unbound sinks", ->
    it "should fail if any unbound sources", ->

    it "should fail if TextLine doesn't have 0, 1, 2 fields", ->
      expect_bad_flow "TextLine can only have at most two fields (one for offset, one for line)", ($) ->
        $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1", "line_2"))

        a1 = $.assembly 'input', ->

        $.sink 'input', $.tap("output", new schemes.TextLine())

     it "should propagate fields correctly for map spec", ->
        with_test_flow ($) ->
          $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line"))

          $.assembly 'input', ->
            insert_step = $.each_step { upcase: null, offset: "line_number", line: null }, (tuple, emitter) ->
              emitter({ upcase: line.toUpperCase() })

            expect(insert_step.incoming.sort()).toEqual ["line", "offset"].sort()
            expect(insert_step.outgoing.sort()).toEqual ["line_number", "upcase"].sort()

     it "should exception if trying to rename an invalid field", ->
        expect_bad_flow "Invalid field bogus being renamed to line_number", ($) ->
          $.source 'input', $.tap("listings.txt", new schemes.TextLine("offset", "line"))

          $.assembly 'input', ->
            $.each_step { bogus: "line_number", line: null }, (tuple, emitter) ->
              emitter({ upcase: line.toUpperCase() })

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

